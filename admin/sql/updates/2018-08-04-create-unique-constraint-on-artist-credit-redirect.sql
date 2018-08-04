BEGIN;

ALTER TABLE artist_credit_redirect ADD CONSTRAINT artist_credit_redirect_uniq UNIQUE (artist_credit_cluster_id, artist_mbids_array);

COMMIT;
