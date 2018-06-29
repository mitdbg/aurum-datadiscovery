from knowledgerepr import fieldnetwork
from neo4j.v1 import GraphDatabase
from api.apiutils import Relation
import sys


def serialize_network_to_neo4j(path_to_serialized_model,server="bolt://neo4j:7687",user="neo4j",pwd="aurum"):
    ## read the network
    network=filednetwork.deserialize_network(path_to_serialized_model)
    
    ## connect to neo4j bolt
    driver = GraphDatabase.driver(server, auth=(user, pwd))

    hit_pattern=re.compile("Hit\(nid='(.*)', db_name='.*', source_name='(.*)', field_name='(.*)', score=(.*)\)")
    nid_hash={}

    ## insert CONTENT_SIM
    for x in network.enumerate_relation(Relation.CONTENT_SIM):
        a=hit_pattern.match(x.split("-")[0].lstrip())
        b=hit_pattern.match(x.split("-")[1].lstrip())
    
        ## insert to neo4j, open a session
        with driver.session() as session:
            if a.groups()[0] not in nid_hash.keys():
                session.run("CREATE (n:Node {nid:$nid,source:$source,field:$field,score:$score}) RETURN id(n)", 
                           nid=a.groups()[0],source=a.groups()[1],field=a.groups()[2],score=a.groups()[3],)
                nid_hash[a.groups()[0]]=nid_hash.get(a.groups()[0],0)+1
            if b.groups()[0] not in nid_hash.keys():
                session.run("CREATE (n:Node {nid:$nid,source:$source,field:$field,score:$score}) RETURN id(n)", 
                           nid=b.groups()[0],source=b.groups()[1],field=b.groups()[2],score=b.groups()[3],)
                nid_hash[b.groups()[0]]=nid_hash.get(b.groups()[0],0)+1

            ## insert relation
            session.run("MATCH (a:Node),(b:Node) WHERE a.nid=$nid_a AND b.nid=$nid_b CREATE (a)-[r1:CONTENT_SIM]->(b) RETURN type(r1)",nid_a=a.groups()[0],nid_b=b.groups()[0]).single().value()


