#!/bin/bash

: <<'DOCSTRING'
This script runs batched insertions of reply chains into a PostgreSQL database.  
It resumes from the last completed batch (based on the `root_id_progress` table)  
and processes batches until none remain in `root_ids`.

Key features:
- Uses recursive CTEs to build two-author reply chains from `filter` and `message_links`.
- Deduplicates messages by root and depth, merges same-author turns, and formats text in ChatML style.
- Inserts completed chains into the `chains` table with assistant/user role counts.
- Marks each batch as finished in `root_id_progress` for safe resuming.

Configuration:
- `PGUSER`: The Postgres user to connect with (default: `postgres`).  
- `DB`: The target database name (default: `maniac`).  
  Change these variables at the top of the script if your environment differs.

The script assumes the presence of:
- `root_ids` table with `batch` column,  
- `root_id_progress` table to track progress,  
- `filter` and `message_links` for building chains.

Exits when no more batches exist in `root_ids`.
DOCSTRING

set -euo pipefail

PGUSER=postgres
DB=maniac

echo "🚀 Starting batched chain insert runner..."

# Get last completed batch
NEXT_BATCH=$(sudo -u postgres psql -U "$PGUSER" -d "$DB" -tAc \
  "SELECT COALESCE(MAX(batch), 0) + 1 FROM root_id_progress WHERE finished = true;")

while true; do
  EXISTS=$(sudo -u postgres psql -U "$PGUSER" -d "$DB" -tAc \
    "SELECT 1 FROM root_ids WHERE batch = $NEXT_BATCH LIMIT 1;")

  if [[ -z "$EXISTS" ]]; then
    echo "No more batches left. Exiting."
    break
  fi

  echo "Running batch $NEXT_BATCH..."
  START=$(date +%s)

  sudo -u postgres psql -U "$PGUSER" -d "$DB" <<EOF
SET max_parallel_workers_per_gather = 16;
SET max_parallel_workers = 16;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET work_mem = '2GB';
SET maintenance_work_mem = '8GB';



WITH RECURSIVE limited_roots AS (
  SELECT m.message_id, m.author_id, m.content
  FROM message_links ml
  JOIN filter m ON m.message_id = ml.reply_to
  WHERE m.message_id IN (
    SELECT message_id FROM root_ids WHERE batch = $NEXT_BATCH
  )
),
chain_roots_raw AS (
  -- Original recursion: all possible paths
  SELECT r.message_id, r.author_id, r.content, r.message_id AS root_id, 1 AS depth
  FROM limited_roots r
  UNION ALL
  SELECT m.message_id, m.author_id, m.content, c.root_id, c.depth + 1
  FROM message_links ml
  JOIN chain_roots_raw c ON ml.reply_to = c.message_id
  JOIN filter m ON m.message_id = ml.message_id
),
chain_roots AS (
  -- Deduplicate by root_id & message_id keeping only the longest depth (max depth)
  SELECT DISTINCT ON (root_id, message_id) *
  FROM chain_roots_raw
  ORDER BY root_id, message_id, depth DESC
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY root_id ORDER BY depth) - 1 AS turn_index
  FROM chain_roots
),
ranked_with_authors AS (
  SELECT r.*,
         (
           SELECT COUNT(DISTINCT author_id)
           FROM ranked r2
           WHERE r2.root_id = r.root_id
             AND r2.turn_index <= r.turn_index
         ) AS distinct_authors_so_far
  FROM ranked r
),
cut_chains AS (
  SELECT *
  FROM ranked_with_authors
  WHERE distinct_authors_so_far <= 2
),
valid_cut_chains AS (
  SELECT *
  FROM cut_chains
  WHERE root_id IN (
    SELECT root_id
    FROM cut_chains
    GROUP BY root_id
    HAVING COUNT(DISTINCT author_id) = 2
  )
),
chain_lengths AS (
  SELECT root_id, COUNT(*) AS chain_len
  FROM valid_cut_chains
  GROUP BY root_id
  HAVING COUNT(*) >= 2
),
chains_with_len AS (
  SELECT c.*, cl.chain_len, MAX(c.turn_index) OVER (PARTITION BY c.root_id) AS max_turn
  FROM valid_cut_chains c
  JOIN chain_lengths cl ON cl.root_id = c.root_id
),

with_roles AS (
  SELECT
    c.root_id AS chain_id,
    c.turn_index,
    c.author_id,
    c.content,
    c.message_id,
    f.guild_id,
    f.channel_id,
    CASE
      WHEN (c.turn_index % 2) = 0 THEN 'user'
      ELSE 'assistant'
    END AS base_role
  FROM chains_with_len c
  JOIN filter f ON f.message_id = c.message_id
),

with_final_roles AS (
  SELECT *,
    CASE
      WHEN last_role = 'user' THEN
        CASE base_role
          WHEN 'user' THEN 'assistant'
          WHEN 'assistant' THEN 'user'
          ELSE base_role
        END
      ELSE base_role
    END AS role
  FROM (
    SELECT *,
      FIRST_VALUE(base_role) OVER (PARTITION BY chain_id ORDER BY turn_index DESC) AS last_role
    FROM with_roles
  ) sub
),

group_keyed AS (
  SELECT *,
         (turn_index - ROW_NUMBER() OVER (PARTITION BY chain_id, author_id ORDER BY turn_index)) AS group_key
  FROM with_final_roles
),

merged AS (
  SELECT
    chain_id,
    MIN(turn_index) AS min_index,
    role,
    guild_id,
    channel_id,
    STRING_AGG(content, E'\n' ORDER BY turn_index) AS merged_content
  FROM group_keyed
  GROUP BY chain_id, role, author_id, group_key, guild_id, channel_id
),

final_chains AS (
  SELECT
    chain_id,
    ROW_NUMBER() OVER (PARTITION BY chain_id ORDER BY min_index) - 1 AS turn_index,
    role,
    merged_content,
    guild_id,
    channel_id
  FROM merged
),

inserted AS (
  INSERT INTO chains (text, guild_id, channel_id, assistant_turns)
  SELECT
    full_text,
    MIN(guild_id),
    MIN(channel_id),
    (length(full_text) - length(REPLACE(full_text, '<|im_start|>assistant', ''))) / length('<|im_start|>assistant') AS assistant_turns
  FROM (
    SELECT
      chain_id,
      STRING_AGG('<|im_start|>' || role || E'\n' || merged_content || '<|im_end|>', E'\n' ORDER BY turn_index) || '<|end_of_text|>' AS full_text,
      guild_id,
      channel_id
    FROM final_chains
    GROUP BY chain_id, guild_id, channel_id
  ) t
  GROUP BY chain_id, full_text
  RETURNING 1
)

-- Summary
SELECT
  $NEXT_BATCH AS batch,
  (SELECT COUNT(*) FROM limited_roots) AS first_level_chains,
  (SELECT COUNT(DISTINCT root_id) FROM ranked) AS chains_after_recursion,
  (SELECT COUNT(DISTINCT root_id) FROM valid_cut_chains) AS chains_with_2_authors,
  (SELECT COUNT(DISTINCT chain_id) FROM final_chains) AS chains_ready_to_insert,
  (SELECT COUNT(*) FROM inserted) AS inserted_rows;
EOF

  # Mark batch as complete
  sudo -u postgres psql -U "$PGUSER" -d "$DB" -c \
    "INSERT INTO root_id_progress (batch, finished) VALUES ($NEXT_BATCH, true)
     ON CONFLICT (batch) DO UPDATE SET finished = true;"

  END=$(date +%s)
  echo "📦 Finished batch in $((END - START))s"

  ((NEXT_BATCH++))
done
