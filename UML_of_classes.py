from rdflib import Namespace 
from rdflib.namespace import FOAF
from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper
import pandas as pd
import numpy as np
import csv


from rdflib import Namespace, URIRef, RDF, Graph, Literal
from rdflib.namespace import FOAF
import pandas as pd

class Handler:  # Chiara
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        if isinstance(pathOrUrl, str):
            self.dbPathOrUrl = pathOrUrl
            return True
        else:
            return False

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass

class MetadataUploadHandler(UploadHandler):  # Chiara
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        my_graph = Graph()
        # Read the data from the csv file and store them into a dataframe
        venus = pd.read_csv(path,
                            keep_default_na=False,
                            dtype={
                                "Id": "string",
                                "Type": "string",
                                "Title": "string",
                                "Date": "string",
                                "Author": "string",
                                "Owner": "string",
                                "Place": "string",
                            })
    
        # Define namespaces
        base_url = Namespace("http://github.com/HelloKittyDataClan/DSexam/")
        db = Namespace("http//dbpedia.org/resource/")
        schema = Namespace("http://schema.org/")

        # Create Graph
        my_graph.bind("base_url", base_url)
        my_graph.bind("db", db)
        my_graph.bind("FOAF", FOAF)
        my_graph.bind("schema", schema)
        
        # Define classes about Cultural Object
        Person = URIRef(FOAF + "Person")
        NauticalChart = URIRef(base_url + "NauticalChart")
        ManuscriptPlate = URIRef(base_url + "ManuscriptPlate")
        ManuscriptVolume = URIRef(base_url + "ManuscriptVolume")
        PrintedVolume = URIRef(base_url + "PrintedVolume")
        PrintedMaterial = URIRef(base_url + "PrintedMaterial")
        Herbarium = URIRef(db + "Herbarium")
        Specimen = URIRef(base_url + "Specimen")
        Painting = URIRef(db + "Painting")
        Model = URIRef(db + "Model")
        Map = URIRef(db + "Map")

        # Attributes related to classes
        title = URIRef(schema + "title")
        date = URIRef(schema + "dateCreated")
        place = URIRef(schema + "itemLocation")
        id = URIRef(schema + "identifier")
        owner = URIRef(base_url + "owner")

        # Relation with authors among classes
        relAuthor = URIRef(schema + "author")

        # Attributes related to the class Person
        name = URIRef(FOAF + "name")

        # Add to the graph the Cultural Object
        for idx, row in venus.iterrows():
            loc_id = "culturalobject-" + str(idx)
            subj = URIRef(base_url + loc_id)

            # Assign a resource class to the object
            if row["Type"] != "":
                if row["Type"].lower() == "nautical chart":
                    my_graph.add((subj, RDF.type, NauticalChart))
                elif row["Type"].lower() == "manuscript plate":
                    my_graph.add((subj, RDF.type, ManuscriptPlate))
                elif row["Type"].lower() == "manuscript volume":
                    my_graph.add((subj, RDF.type, ManuscriptVolume))
                elif row["Type"].lower() == "printed volume":
                    my_graph.add((subj, RDF.type, PrintedVolume))
                elif row["Type"].lower() == "printed material":
                    my_graph.add((subj, RDF.type, PrintedMaterial))
                elif row["Type"].lower() == "herbarium":
                    my_graph.add((subj, RDF.type, Herbarium))
                elif row["Type"].lower() == "specimen":
                    my_graph.add((subj, RDF.type, Specimen))
                elif row["Type"].lower() == "painting":
                    my_graph.add((subj, RDF.type, Painting))
                elif row["Type"].lower() == "model":
                    my_graph.add((subj, RDF.type, Model))
                elif row["Type"].lower() == "map":
                    my_graph.add((subj, RDF.type, Map))

            # Assign identifier
            if row["Id"] != "":
                my_graph.add((subj, id, Literal(str(row["Id"]))))
            # Assign title
            if row["Title"] != "":
                my_graph.add((subj, title, Literal(str(row["Title"]))))
            # Assign date
            if row["Date"] != "":
                my_graph.add((subj, date, Literal(str(row["Date"]))))
            # Assign owner
            if row["Owner"] != "":
                my_graph.add((subj, owner, Literal(str(row["Owner"]))))
            # Assign place
            if row["Place"] != "":
                my_graph.add((subj, place, Literal(str(row["Place"]))))

        # Populating the graph with all the people
        author_id_mapping = dict()   
        object_mapping = dict()
        people_counter = 0

        for idx, row in venus.iterrows():
            if row["Author"] != "":
                author_list = row["Author"].split(";")
                for author in author_list:
                    if "(" in author and ")" in author:
                        split_index = author.index("(")
                        author_name = author[:split_index - 1].strip()
                        author_id = author[split_index + 1:-1].strip()
                
                        object_id = row["Id"]

                        if author_id in author_id_mapping:
                            person_uri = author_id_mapping[author_id]
                        else:
                            local_id = "person-" + str(people_counter)
                            person_uri = URIRef(base_url + local_id)
                            my_graph.add((person_uri, RDF.type, Person))
                            my_graph.add((person_uri, id, Literal(author_id)))
                            my_graph.add((person_uri, name, Literal(author_name)))
                            author_id_mapping[author_id] = person_uri
                            people_counter += 1

                        if object_id in object_mapping:
                            object_mapping[object_id].add(person_uri)
                        else:
                            object_mapping[object_id] = {person_uri}

                # Aggiungi l'assegnazione degli autori al grafo
                for object_id, authors in object_mapping.items():
                    for author_uri in authors:
                        my_graph.add((author_uri, relAuthor, URIRef(base_url + object_id)))

        # Store RDF data in SPARQL endpoint
        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl() + "sparql"

        store.open((endpoint, endpoint))  # First parameter reading data, second write database

        for triple in my_graph.triples((None, None, None)):
            store.add(triple)

        store.close()

grp_dbUrl = " http://192.168.1.8:9999/blazegraph/"
metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl(grp_dbUrl)
metadata.pushDataToDb("../data/meta.csv")


# java -server -Xmx1g -jar blazegraph.jar (terminal command to run Blazegraph)

#Bea
class MetadataQueryHandler:
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = dbPathOrUrl
    
    def execute_sparql_query(self, query):
        sparql = SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(SPARQLWrapper.CSV)
        sparql.setQuery(query)
        try:
            ret = sparql.queryAndConvert()
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
        
        df_columns = ret["head"]["vars"]
        df = pd.DataFrame(columns=df_columns)
        for row in ret["results"]["bindings"]:
            row_dict = {column: row[column]["value"] for column in df_columns if column in row}
            df = df.append(row_dict, ignore_index=True)
        return df.replace(np.nan, " ")
    
    def getById(self, id):
        person_query = f"""
        SELECT DISTINCT ?uri ?name ?id WHERE {{
            ?object <https://schema.org/author> ?uri.
            ?uri <https://schema.org/name> ?name.
            ?uri <https://schema.org/identifier> ?id.
            ?uri <https://schema.org/identifier> '{id}'.
        }}"""
        
        object_query = f"""
        SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id WHERE {{
            ?object <https://schema.org/identifier> ?id.
            ?object <https://schema.org/identifier> '{id}'.
            ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type.
            ?object <https://schema.org/name> ?title.
            ?object <https://schema.org/copyrightHolder> ?owner.
            ?object <https://schema.org/spatial> ?place.
            OPTIONAL{{ ?object <https://schema.org/dateCreated> ?date. }}
            OPTIONAL{{ 
                ?object <https://schema.org/author> ?author.
                ?author <https://schema.org/name> ?author_name.
                ?author <https://schema.org/identifier> ?author_id.
            }}
        }}"""
        
        person_df = self.execute_sparql_query(person_query)
        object_df = self.execute_sparql_query(object_query)
        
        if not object_df.empty:
            return object_df
        return person_df

   

def getAllPeople(self): # Definisci la tua query SPARQL
    query = """
    SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
    WHERE {{ 
        ?object <https://schema.org/identifier> ?id. 
        ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. 
        ?object <https://schema.org/name> ?title. 
        ?object <https://schema.org/copyrightHolder> ?owner. 
        ?object <https://schema.org/spatial> ?place. 
        OPTIONAL{{ ?object <https://schema.org/dateCreated> ?date. }} 
        OPTIONAL{{ 
            ?object <https://schema.org/author> ?author. 
            ?author <https://schema.org/name> ?author_name. 
            ?author <https://schema.org/identifier> ?author_id.
        }}
    }}
    """

   

    def getAllCulturalHeritageObjects(self):
         query = """
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE { 
                ?object <https://schema.org/identifier> ?id. 
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. 
                ?object <https://schema.org/name> ?title. 
                ?object <https://schema.org/copyrightHolder> ?owner. 
                ?object <https://schema.org/spatial> ?place. 
            OPTIONAL{ ?object <https://schema.org/dateCreated> ?date. } 
            OPTIONAL{ 
                ?object <https://schema.org/author> ?author. 
                ?author <https://schema.org/name> ?author_name. 
                ?author <https://schema.org/identifier> ?author_id.
            }}
            """
    return self.execute_sparql_query(query)


    def getAuthorsOfCulturalHeritageObject(self, object_id):
        query = f"""
        SELECT DISTINCT ?author ?author_name ?author_id 
        WHERE {{ 
            ?object <https://schema.org/identifier> '{object_id}'. 
            ?object <https://schema.org/author> ?author. 
            ?author <https://schema.org/name> ?author_name. 
            ?author <https://schema.org/identifier> ?author_id. 
        }}"""
        return self.execute_sparql_query(query)
    

    def getAuthorsOfCulturalHeritageObject(self, object_id: str): 
        query = """
            SELECT DISTINCT ?author ?author_name ?author_id 
            WHERE { 
                ?object <https://schema.org/identifier> '%s'. 
                ?object <https://schema.org/author> ?author. 
                ?author <https://schema.org/name> ?author_name. 
                ?author <https://schema.org/identifier> ?author_id. 
                }""" % object_id
        return self.execute_sparql_query(query)



    def getCulturalHeritageObjectsAuthoredBy(self, personId):
        query = f"""
    SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
    WHERE {{ 
        ?object <https://schema.org/identifier> ?id. 
        ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. 
        ?object <https://schema.org/name> ?title. 
        ?object <https://schema.org/copyrightHolder> ?owner. 
        ?object <https://schema.org/spatial> ?place. 
        ?object <https://schema.org/author> ?author. 
        ?author <https://schema.org/name> ?author_name.
        ?author <https://schema.org/identifier> ?author_id.
        OPTIONAL{{ ?object <https://schema.org/dateCreated> ?date. }}
        FILTER CONTAINS(?author_id, '{personId}')
    }}"""
        return self.execute_sparql_query(query)
    

# Configurazione dell'URL di Blazegraph per me
blazegraph_url = "http://192.168.1.152:8080/blazegraph/namespace/kb/sparql"


# Creare un'istanza di MetadataQueryHandler per me
grp_endpoint = "http://192.168.1.152:8080/blazegraph/namespace/kb/sparql"
sparql_query_handler = MetadataQueryHandler(dbPathOrUrl=grp_endpoint)



 

