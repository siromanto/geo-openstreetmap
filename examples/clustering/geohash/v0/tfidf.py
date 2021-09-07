BASE_32 = ('0','1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
           'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
           )

LEVEL_1 = (
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
)

QUERY = """
WITH data AS (
SELECT
  *
FROM `gcp-pdp-osm-dev.geohash_v1.level_7_terms_partitioned`
WHERE SUBSTR(geohash, 1, 2) = "{}"
)
,features_matrix AS (
SELECT geohash, CONCAT(gf.layer_code, "_", gf.layer_class, "_", gf.layer_name) as word
FROM `gcp-pdp-osm-dev.geofabrik.layers` gf
CROSS JOIN (SELECT geohash FROM data GROUP BY geohash)
ORDER BY geohash, word
)
SELECT
  fm.geohash as geohash, ARRAY_AGG(IFNULL(tf_idf.tfidf, 0.0) ORDER BY fm.word) as tfidf_vec
FROM features_matrix fm
LEFT JOIN `gcp-pdp-osm-dev.geohash_v1.level_7_vectors_tfidf_raw` tf_idf ON tf_idf.term = fm.word AND tf_idf.geohash = fm.geohash
GROUP BY geohash
"""

CMD = 'bq query --batch --append_table --destination_table gcp-pdp-osm-dev:geohash_v1.level_7_vectors_tfidf' \
      ' --nosynchronous_mode --nouse_legacy_sql \'{}\''

if __name__ == '__main__':
    count = 0
    with open('query_run.sh', 'w') as f:

        for i in LEVEL_1:
            for j in BASE_32:
                geohash = i + j
                query = QUERY.format(geohash).replace('\n', ' ')
                cmd = CMD.format(query) + '\n'
                count +=1
                # print(cmd)
                f.write(cmd)
        print(f"rows count - {count}")
