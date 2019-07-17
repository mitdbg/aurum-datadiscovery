from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD.dod import DoD
from DoD import data_processing_utils as dpu

from tqdm import tqdm

import os


def create_folder(base_folder, name):
    op = base_folder + name
    os.makedirs(op)
    return op


def run_dod(dod, attrs, values, output_path, max_hops=2):
    view_metadata_mapping = dict()
    i = 0
    for mjp, attrs_project, metadata in dod.virtual_schema_iterative_search(attrs, values, max_hops=max_hops,
                                                                            debug_enumerate_all_jps=False):
        proj_view = dpu.project(mjp, attrs_project)

        if output_path is not None:
            view_path = output_path + "/view_" + str(i)
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this
            # store metadata associated to that view
            view_metadata_mapping[view_path] = metadata

        i += 1


def assemble_views():
    # have a way of generating the views for each query-view in a different folder
    for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_many):
        print("Running query: " + str(qv_name))
        # Create a folder for each query-view
        output_path = create_folder(eval_folder, "many/" + qv_name)
        print("Out path: " + str(output_path))
        run_dod(dod, qv_attr, qv_values, output_path=output_path)

    for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_few):
        print("Running query: " + str(qv_name))
        # Create a folder for each query-view
        output_path = create_folder(eval_folder, "few/" + qv_name)
        print("Out path: " + str(output_path))
        run_dod(dod, qv_attr, qv_values, output_path=output_path)

if __name__ == "__main__":
    print("DoD evaluation")

    # Eval parameters
    eval_folder = "dod_evaluation/vassembly/"

    query_view_definitions_many = [
        ("qv2", ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"],
         ["", "", "", ""]),
        ("qv4", ["Email Address", "Department Full Name"],
         ["madden@csail.mit.edu", ""]),
        ("qv5", ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"],
         ["", "", "", ""])
    ]

    query_view_definitions_few = [
        ("qv1", ["Iap Category Name", "Person Name", "Person Email"],
         ["Engineering", "", ""]),
        ("qv3", ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"],
         ["Madden", "Ray and Maria Stata Center", "", "Dept of Electrical Engineering & Computer Science"]),
    ]

    # Configure DoD
    path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/mitdwh/"
    sep = ","
    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    dod = DoD(network=network, store_client=store_client, csv_separator=sep)

    assemble_views()

    # then have a way for calling 4c on each folder -- on all folders
