import dataset
import sqlalchemy


class PGStore:

    def __init__(self, db_ip="localhost", db_port="5432", db_name=None, db_user=None, db_passwd=None):
        self.db_ip = db_ip
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.con = self.connect()
        #self.nodes_table = self.con['nodes']
        self.nodes_table = self.con.create_table('nodes', primary_id='node_id', primary_type='Integer')
        #self.edges_table = self.con['edges']
        self.edges_table = self.con.create_table('edges')

    def connect(self):
        connection_string = "postgresql://" +\
                             self.db_user + ":" + \
                             self.db_passwd + "@" + \
                             self.db_ip + ":" + \
                             self.db_port + "/" + \
                             self.db_name
        con = dataset.connect(connection_string)
        return con

    """
    WRITE
    """

    def new_node(self, node_id=None, uniqueness_ratio=None):
        try:
            self.nodes_table.insert(dict(node_id=node_id, uniqueness_ratio=uniqueness_ratio))
        except sqlalchemy.exc.IntegrityError:
            print("attempt to insert duplicate key")

    def new_edge(self, source_node_id=None, target_node_id=None, relation_type=None, weight=None):
        try:
            self.edges_table.insert(dict(source_node_id=source_node_id,
                                         target_node_id=target_node_id,
                                         relation_type=relation_type,
                                         weight=weight))
        except sqlalchemy.exc.IntegrityError:
            print("attempt to insert duplicate key")

    """
    READ
    """

    def connected_to(self, nid):
        results = self.edges_table.find(source_node_id=nid)
        return results

if __name__ == "__main__":
    print("pg store")

    store = PGStore("localhost", "5432", "test10", "postgres", "admin")

    import networkx as nx

    nodes = 10
    edge_propability = 0.5  # fairly populated graph
    random_g = nx.fast_gnp_random_graph(nodes, edge_propability)

    for src_id, tgt_id in random_g.edges():
        store.new_node(src_id)
        store.new_node(tgt_id)
        store.new_edge(src_id, tgt_id)

"""  TEST
with recursive r as (select source_node_id, target_node_id from edges where source_node_id = 0
union all
select edges.source_node_id, edges.target_node_id from edges inner join r on edges.source_node_id = r.target_node_id)
select source_node_id from r;
"""

""" WORKING VERSION
WITH RECURSIVE transitive_closure (src, tgt, path_string) AS
  (

   SELECT e.source_node_id, e.target_node_id,
   e.source_node_id || '.' || e.target_node_id || '.' AS path_string
   FROM edges e
   WHERE e.source_node_id = 0 --source_node

   UNION


   SELECT tc.src, e.target_node_id, tc.path_string || e.target_node_id || '.' AS path_string
   FROM edges AS e JOIN transitive_closure AS tc ON e.source_node_id = tc.tgt
   WHERE tc.path_string NOT LIKE '%' || e.target_node_id || '.%'
  )
  SELECT *
  FROM transitive_closure tc
  WHERE tc.tgt = 7;
"""