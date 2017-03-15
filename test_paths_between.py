from main import init_system
# from pathselection import joinpathselection
#from integrating_civilizer import *
from api.apiutils import Relation
api, reporting = init_system("../testmodel/")


t1 = "Employee_directory.csv"
t2 = "Sis_department.csv"

def joinPaths(table1, table2):
    drs_t1 = api.drs_from_table(table1)
    drs_t2 = api.drs_from_table(table2)
    drs_t1.set_table_mode()
    drs_t2.set_table_mode()
    #res = api.paths_between(drs_t1, drs_t2, Relation.PKFK, max_hops=1)
    res = api.paths_between(drs_t1, drs_t2, Relation.CONTENT_SIM, max_hops=2)
    return res

if __name__ == "__main__":
    # reporting.print_content_sim_relations()
    res = joinPaths(t1, t2)
    #format_join_paths(res)