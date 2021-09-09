import datetime
import os
import airflow
import json

from airflow.contrib.operators import kubernetes_pod_operator

from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import bigquery_operator
from airflow.operators import bash_operator

from utils import bq_utils
from utils import gcs_utils

year_start = datetime.datetime(2020, 1, 1)


with open('credentials/osm_variables.json') as f:
    conf = json.load(f)

project_id = conf["planet"]["project_id"]
bq_dataset_to_export = conf["planet"]

osm_to_features_image = conf["planet"]

gcs_work_bucket = conf["planet"]
osm_to_nodes_ways_relations_image = conf["planet"]

gke_main_cluster_name = conf["planet"]["gke_main_cluster_name"]
gke_zone = conf["planet"]["gke_zone"]

addt_sn_gke_pool = conf["planet"]["addt_sn_gke_pool"]
addt_sn_gke_pool_machine_type = conf["planet"]["addt_sn_gke_pool_machine_type"]
addt_sn_gke_pool_disk_size = conf["planet"]["addt_sn_gke_pool_disk_size"]
addt_sn_gke_pool_num_nodes = conf["planet"]["addt_sn_gke_pool_num_nodes"]
addt_sn_gke_pool_max_num_treads = conf["planet"]["addt_sn_gke_pool_max_num_treads"]

addt_mn_gke_pool = conf["planet"]["addt_mn_gke_pool"]
addt_mn_gke_pool_machine_type = conf["planet"]["addt_mn_gke_pool_machine_type"]
addt_mn_gke_pool_disk_size = conf["planet"]["addt_mn_gke_pool_disk_size"]
addt_mn_gke_pool_num_nodes = conf["planet"]["addt_mn_gke_pool_num_nodes"]
addt_mn_pod_requested_memory = conf["planet"]["addt_mn_pod_requested_memory"]

generate_layers_image = conf["planet"]["generate_layers_image"]
test_osm_gcs_uri = ""

feature_union_bq_table_name = "planet_features"
json_results_gcs_uri = "gs://{}/results_jsonl/".format(gcs_work_bucket)

local_data_dir_path = "/home/airflow/gcs/dags/"

default_args = {
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=15),
    'start_date': year_start
}

max_bad_records_for_bq_export = 10000

create_additional_pool_cmd = '''
    gcloud container node-pools create {{ params.POOL_NAME }} \
        --cluster {{ params.GKE_CLUSTER_NAME }} \
        --project {{ params.PROJECT_ID }} \
        --zone {{ params.GKE_ZONE }} \
        --machine-type {{ params.POOL_MACHINE_TYPE }} \
        --num-nodes {{ params.POOL_NUM_NODES }} \
        --disk-size {{ params.POOL_DISK_SIZE }} \
        --disk-type pd-ssd \
        --scopes gke-default,storage-rw,bigquery
'''
delete_additional_pool_cmd = '''
    gcloud container node-pools delete {{ params.POOL_NAME }} \
        --zone {{ params.GKE_ZONE }} \
        --cluster {{ params.GKE_CLUSTER_NAME }} \
        -q
'''
nodes_ways_relations_elements = ["nodes", "ways", "relations"]

with airflow.DAG(
        'osm_to_big_query_planet',
        catchup=False,
        default_args=default_args,
        schedule_interval=None) as dag:

    def file_to_json(file_path):
        with open(file_path) as f:
            json_dict = json.load(f)
        return json_dict


    def file_to_text(file_path):
        with open(file_path) as f:
            return "".join(f.readlines())


    def create_gke_affinity_with_pool_name(pool_name):
        return {'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'cloud.google.com/gke-nodepool',
                        'operator': 'In',
                        'values': [pool_name]
                    }]
                }]
            }
        }}


    src_osm_gcs_uri = test_osm_gcs_uri if test_osm_gcs_uri else "gs://{}/{}".format('{{ dag_run.conf.bucket }}',
                                                                                    '{{ dag_run.conf.name }}')
    # TASK #1. update-history-index
    create_sn_additional_pool_task = bash_operator.BashOperator(task_id="create-sn-additional-pool",
                                                                bash_command=create_additional_pool_cmd,
                                                                params={"POOL_NAME": addt_sn_gke_pool,
                                                                        "GKE_CLUSTER_NAME": gke_main_cluster_name,
                                                                        "PROJECT_ID": project_id,
                                                                        "GKE_ZONE": gke_zone,
                                                                        "POOL_MACHINE_TYPE": addt_sn_gke_pool_machine_type,
                                                                        "POOL_NUM_NODES": addt_sn_gke_pool_num_nodes,
                                                                        "POOL_DISK_SIZE": addt_sn_gke_pool_disk_size
                                                                        })
    # TASK #2. update-history-index
    create_mn_additional_pool_task = bash_operator.BashOperator(task_id="create-mn-additional-pool",
                                                                bash_command=create_additional_pool_cmd,
                                                                params={"POOL_NAME": addt_mn_gke_pool,
                                                                        "GKE_CLUSTER_NAME": gke_main_cluster_name,
                                                                        "PROJECT_ID": project_id,
                                                                        "GKE_ZONE": gke_zone,
                                                                        "POOL_MACHINE_TYPE": addt_mn_gke_pool_machine_type,
                                                                        "POOL_NUM_NODES": addt_mn_gke_pool_num_nodes,
                                                                        "POOL_DISK_SIZE": addt_mn_gke_pool_disk_size
                                                                        })
    create_mn_additional_pool_task.set_upstream(create_sn_additional_pool_task)

    # TASK #3. osm_to_features
    osm_to_features_branches = [("multipolygons", ["multipolygons"]),
                                ("other-features", ["other_relations", "points", "multilinestrings", "lines"])]

    features = []
    for osm_to_features_branch in osm_to_features_branches:
        features.extend(osm_to_features_branch[1])

    osm_to_features_tasks_data = []
    for branch_tuple in osm_to_features_branches:
        branch_name, branch_features_to_process = branch_tuple

        layers = ",".join(branch_features_to_process)
        osm_to_features_task = kubernetes_pod_operator.KubernetesPodOperator(
            task_id='osm-to-features--{}'.format(branch_name),
            name='osm-to-features--{}'.format(branch_name),
            namespace='default',
            image_pull_policy='Always',
            env_vars={'SRC_OSM_GCS_URI': src_osm_gcs_uri,
                      'FEATURES_DIR_GCS_URI': json_results_gcs_uri,
                      'LAYERS': layers},
            image=osm_to_features_image,
            resources={"request_memory": addt_mn_pod_requested_memory},
            affinity=create_gke_affinity_with_pool_name(addt_mn_gke_pool),
            execution_timeout=datetime.timedelta(days=4)
        )

        osm_to_features_tasks_data.append((osm_to_features_task, branch_name))
        osm_to_features_task.set_upstream(create_mn_additional_pool_task)

    # TASK #4.N. {}_feature_json_to_bq
    features_to_bq_tasks_data = []
    nodes_schema = file_to_json(local_data_dir_path + 'schemas/features_table_schema.json')
    src_features_gcs_bucket, src_features_gcs_dir = gcs_utils.parse_uri_to_bucket_and_filename(json_results_gcs_uri)
    jsonl_file_names_format = src_features_gcs_dir + 'feature-{}.geojson.csv.jsonl'

    for feature in features:
        task_id = feature + '_feature_json_to_bq'
        source_object = jsonl_file_names_format.format(feature)
        destination_dataset_table = '{}.planet_features_{}'.format(bq_dataset_to_export, feature)

        task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket=src_features_gcs_bucket,
            source_objects=[source_object],
            source_format='NEWLINE_DELIMITED_JSON',
            destination_project_dataset_table=destination_dataset_table,
            schema_fields=nodes_schema,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=max_bad_records_for_bq_export,
            dag=dag)
        features_to_bq_tasks_data.append((task, feature, destination_dataset_table))

    # TASK #5. feature_union
    create_features_part_format = file_to_text(local_data_dir_path + 'sql/create_features_part_format.sql')
    create_features_queries = [create_features_part_format.format(task_tuple[1], task_tuple[2])
                               for task_tuple in features_to_bq_tasks_data]
    feature_union_query = bq_utils.union_queries(create_features_queries)

    destination_table = "{}.{}".format(bq_dataset_to_export, feature_union_bq_table_name)
    feature_union_task = bigquery_operator.BigQueryOperator(
        task_id='feature_union',
        bql=feature_union_query,
        destination_dataset_table=destination_table,
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False
    )

    # TASK #6. osm_to_nodes_ways_relations
    osm_to_nodes_ways_relations = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='osm-to-nodes-ways-relations',
        name='osm-to-nodes-ways-relations',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'PROJECT_ID': project_id,
                  'SRC_OSM_GCS_URI': src_osm_gcs_uri,
                  'NODES_WAYS_RELATIONS_DIR_GCS_URI': json_results_gcs_uri,
                  'NUM_THREADS': addt_sn_gke_pool_max_num_treads},
        image=osm_to_nodes_ways_relations_image,
        affinity=create_gke_affinity_with_pool_name(addt_sn_gke_pool),
        execution_timeout=datetime.timedelta(days=4)
    )
    osm_to_nodes_ways_relations.set_upstream(create_mn_additional_pool_task)

    # TASK #7.N. nodes_ways_relations_to_bq
    nodes_ways_relations_tasks_data = []

    schemas = [file_to_json(local_data_dir_path + 'schemas/{}_table_schema.json'.format(element))
               for element in nodes_ways_relations_elements]

    elements_and_schemas = [(nodes_ways_relations_elements[i], schemas[i])
                            for i in range(len(nodes_ways_relations_elements))]
    schema = file_to_json(local_data_dir_path + 'schemas/simple_table_schema.json')
    src_nodes_ways_relations_gcs_bucket, src_nodes_ways_relations_gcs_dir = gcs_utils.parse_uri_to_bucket_and_filename(
        json_results_gcs_uri)
    jsonl_file_names_format = src_nodes_ways_relations_gcs_dir + '{}.jsonl'

    for element_and_schema in elements_and_schemas:
        element, schema = element_and_schema
        task_id = element + '_json_to_bq'
        source_object = jsonl_file_names_format.format(element)
        destination_dataset_table = '{}.planet_{}'.format(bq_dataset_to_export, element)

        task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket=src_nodes_ways_relations_gcs_bucket,
            source_objects=[source_object],
            source_format='NEWLINE_DELIMITED_JSON',
            destination_project_dataset_table=destination_dataset_table,
            schema_fields=schema,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=max_bad_records_for_bq_export,
            dag=dag)
        nodes_ways_relations_tasks_data.append((task, element, destination_dataset_table))

    # TASK #8. generate_layers
    generate_layers = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='generate-layers',
        name='generate-layers',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'PROJECT_ID': project_id,
                  'BQ_DATASET_TO_EXPORT': bq_dataset_to_export,
                  'MODE': 'planet'},
        image=generate_layers_image,
        affinity=create_gke_affinity_with_pool_name(addt_sn_gke_pool))

    # TASK #9. join_geometries
    join_geometries_tasks = []
    for element in nodes_ways_relations_elements:
        join_geometries_format = file_to_text(local_data_dir_path + 'sql/join_{}_geometries.sql'.format(element))
        join_geometries_query = join_geometries_format.format(bq_dataset_to_export, bq_dataset_to_export,
                                                                bq_dataset_to_export)
        destination_table = "{}.{}".format(bq_dataset_to_export, "planet_{}".format(element))
        join_geometries_task = bigquery_operator.BigQueryOperator(
            task_id='join-{}-geometries'.format(element),
            bql=join_geometries_query,
            destination_dataset_table=destination_table,
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False
        )
        join_geometries_tasks.append(join_geometries_task)
    generate_layers.set_downstream(join_geometries_tasks)

    # TASK #10. delete_sn_additional_pool
    delete_sn_additional_pool_task = bash_operator.BashOperator(task_id="delete-sn-additional-pool",
                                                                bash_command=delete_additional_pool_cmd,
                                                                params={"POOL_NAME": addt_sn_gke_pool,
                                                                        "GKE_CLUSTER_NAME": gke_main_cluster_name,
                                                                        "GKE_ZONE": gke_zone},
                                                                trigger_rule="all_done")
    delete_sn_additional_pool_task.set_upstream(join_geometries_tasks)

    # TASK #11. generate_layers
    delete_mn_additional_pool_task = bash_operator.BashOperator(task_id="delete-mn-additional-pool",
                                                                bash_command=delete_additional_pool_cmd,
                                                                params={"POOL_NAME": addt_mn_gke_pool,
                                                                        "GKE_CLUSTER_NAME": gke_main_cluster_name,
                                                                        "GKE_ZONE": gke_zone},
                                                                trigger_rule="all_done")
    delete_mn_additional_pool_task.set_upstream(delete_sn_additional_pool_task)

    # Graph building
    branch_and_features_to_bq_tasks = {}
    for osm_to_features_branch in osm_to_features_branches:
        branch_name, branch_features = osm_to_features_branch

        tasks_per_features_branch = []
        for feature_tasks_item in features_to_bq_tasks_data:
            if feature_tasks_item[1] in branch_features:
                tasks_per_features_branch.append(feature_tasks_item[0])
        branch_and_features_to_bq_tasks[branch_name] = tasks_per_features_branch

    features_to_bq_tasks = [features_to_bq_tasks_item[0] for features_to_bq_tasks_item in features_to_bq_tasks_data]
    nodes_ways_relations_tasks = [nodes_ways_relations_tasks_item[0]
                                  for nodes_ways_relations_tasks_item in nodes_ways_relations_tasks_data]

    for osm_to_feature_task_data_item in osm_to_features_tasks_data:
        task, branch_name = osm_to_feature_task_data_item
        task.set_downstream(branch_and_features_to_bq_tasks[branch_name])

    feature_union_task.set_upstream(features_to_bq_tasks)
    osm_to_nodes_ways_relations.set_downstream(nodes_ways_relations_tasks)
    generate_layers.set_upstream(nodes_ways_relations_tasks + [feature_union_task])
