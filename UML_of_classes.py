from rdflib import Namespace 
from rdflib.namespace import FOAF
from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import numpy as np
import csv
import urllib.request



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
        db = Namespace("https://dbpedia.org/property/")
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
        name = URIRef(FOAF + "name") #uri di FOAF http://xmlns.com/foaf/0.1/

        
        # Add to the graph the Cultural Object
        for idx, row in venus.iterrows():
            loc_id = "culturalobject-" + str(idx)
            subj = URIRef(base_url + loc_id)
            if row["Id"] != "":
                my_graph.add((subj, id, Literal(str(row["Id"]))))
            
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

'''prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix schema: <http://schema.org/>
prefix FOAF: <http://xmlns.com/foaf/0.1/>
       

SELECT ?culturalObjectURI ?culturalObjectId
WHERE {
            ?culturalObjectURI rdf:type ?type .
            FILTER(?type != FOAF:Person)
            ?culturalObjectURI schema:identifier ?culturalObjectId .
        }'''




#parte Bea manca getCulturalHeritageObjectsAuthoredBy(personid: string) : DataFrame la carico domattina quando la revisiono perchè mo so stanca e sta pure mezza disordinata sta implementazione 

class MetadataQueryHandler(Handler):
    def __init__(self, db_url):
        super().__init__()
        self.dbPathOrUrl = db_url

    
    


    def getById(self, id):
        object_query = f"""
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE {{
                ?object <http://schema.org/identifier> "{id}" ;
                        a ?type ;
                        <http://schema.org/title> ?title ;
                        <http://schema.org/dateCreated> ?date ;
                        <http://github.com/HelloKittyDataClan/DSexam/owner> ?owner ;
                        <http://schema.org/itemLocation> ?place .
                OPTIONAL {{
                    ?object <http://schema.org/author> ?author.
                    ?author <http://xmlns.com/foaf/0.1/name> ?author_name.
                    ?author <http://schema.org/identifier> ?author_id.
                }}
            }}
        """
        results = self.execute_sparql_query(object_query)
        return pd.DataFrame(results["results"]["bindings"])
    


    def getAllPeople(self):
        grp_dbUrl = "http://192.168.1.153:9999/blazegraph/"   
        metadata = MetadataUploadHandler()
        metadata.setDbPathOrUrl(grp_dbUrl)
        metadata.pushDataToDb("~/Downloads/Data Science/meta.csv")
        query = """
        PREFIX FOAF: <http://xmlns.com/foaf/0.1/>
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?uri ?author_name ?author_id
        WHERE {
            ?person a FOAF:Person ;
                    schema:identifier ?author_id ;
                    FOAF:name ?author_name .
        }
        """
        results = self.execute_sparql_query(query)
        return pd.DataFrame(results["results"]["bindings"])
    
    
    def getAllCulturalHeritageObjects(self):
        grp_dbUrl = "http://192.168.1.153:9999/blazegraph/"   
        metadata = MetadataUploadHandler()
        metadata.setDbPathOrUrl(grp_dbUrl)
        metadata.pushDataToDb("~/Downloads/Data Science/meta.csv")
        query = """
        PREFIX schema: <http://schema.org/>
        SELECT ?object ?id ?title ?date ?owner ?place WHERE {
            ?object a ?type ;
                    schema:identifier ?id ;
                    schema:title ?title ;
                    schema:dateCreated ?date ;
                    <http://github.com/HelloKittyDataClan/DSexam/owner> ?owner ;
                    schema:itemLocation ?place .
            FILTER (?type IN (
                <http://github.com/HelloKittyDataClan/DSexam/NauticalChart>,
                <http://github.com/HelloKittyDataClan/DSexam/ManuscriptPlate>,
                <http://github.com/HelloKittyDataClan/DSexam/ManuscriptVolume>,
                <http://github.com/HelloKittyDataClan/DSexam/PrintedVolume>,
                <http://github.com/HelloKittyDataClan/DSexam/PrintedMaterial>,
                <https://dbpedia.org/property/Herbarium>,
                <http://github.com/HelloKittyDataClan/DSexam/Specimen>,
                <https://dbpedia.org/property/Painting>,
                <https://dbpedia.org/property/Model>,
                <https://dbpedia.org/property/Map>
            ))
        }
        """

        results = self.execute_sparql_query(query)

        objects = []
        for result in results["results"]["bindings"]:
            obj = {}
            obj["object"] = result["object"]["value"] if "object" in result else None
            obj["id"] = result["id"]["value"] if "id" in result else None
            obj["title"] = result["title"]["value"] if "title" in result else None
            obj["date"] = result["date"]["value"] if "date" in result else None
            obj["owner"] = result["owner"]["value"] if "owner" in result else None
            obj["place"] = result["place"]["value"] if "place" in result else None
            objects.append(obj)

        return pd.DataFrame(objects)
    

    def execute_sparql_query(self, query):
        sparql = SPARQLWrapper(self.dbPathOrUrl + "sparql")
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)
        ret = sparql.queryAndConvert()
        df_columns = ret["head"]["vars"]
        df = pd.DataFrame(columns=df_columns)
        for row in ret["results"]["bindings"]:
            row_dict = {}
            for column in df_columns:
                if column in row:
                    row_dict[column] = row[column]["value"]
                else:
                    row_dict[column] = None
            df = df.append(row_dict, ignore_index=True)
        return df
    

    
    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:
        grp_dbUrl = "http://192.168.1.153:9999/blazegraph/"   
        metadata = MetadataUploadHandler()
        metadata.setDbPathOrUrl(grp_dbUrl)
        metadata.pushDataToDb("~/Downloads/Data Science/meta.csv")
        print("Object ID:", object_id)
        query = f"""
        PREFIX base_url: <http://github.com/HelloKittyDataClan/DSexam/>
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT ?author ?authorName ?entity
        WHERE {{
            ?author schema:author ?entity .
            ?author foaf:name ?authorName .
            ?author schema:identifier ?authorID
        }}
        """
    
        df = self.execute_sparql_query(query)
        if df.empty:
         return pd.DataFrame()

        df['author'] = df['author'].apply(lambda x: x.rsplit('/', 1)[-1])

        all_dfs = []
        for author in df['author']:
            query_1 = f"""
            PREFIX base_url: <http://github.com/HelloKittyDataClan/DSexam/>
            PREFIX schema: <http://schema.org/>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>

            SELECT ?authorName ?authorID ?entity
            WHERE {{
                ?author schema:author ?entity .
                OPTIONAL {{ ?author foaf:name ?authorName }}
                OPTIONAL {{ ?author schema:identifier ?authorID }}
            }}

            """

            df_main = self.query_sparql(query_1)
            if df_main.empty:
             continue

            df_main['entity'] = df_main['entity'].apply(lambda x: x.rsplit('/', 1)[-1])
            df_main = df_main.rename(columns={'name': 'authorName', 'authorID': 'authorID'})  # Rinomina le colonne
            all_dfs.append(df_main)

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        else:
            return pd.DataFrame()


    def getCulturalHeritageObjectsAuthoredBy(self, personid: str) -> pd.DataFrame:
        endpoint_url = self.getDbPathOrUrl() + "sparql"
        sparql = SPARQLWrapper(endpoint_url)

        query = f"""
        PREFIX base_url: <http://github.com/HelloKittyDataClan/DSexam/>
        PREFIX schema: <http://schema.org/>
        PREFIX FOAF: <http://xmlns.com/foaf/0.1/>

        SELECT ?objectId ?title ?date ?place ?owner
        WHERE {{
            ?person FOAF:identifier "{personid}" .
            ?person schema:author ?culturalObject .
            ?culturalObject schema:identifier ?objectId .
            OPTIONAL {{ ?culturalObject schema:title ?title . }}
            OPTIONAL {{ ?culturalObject schema:dateCreated ?date . }}
            OPTIONAL {{ ?culturalObject schema:itemLocation ?place . }}
            OPTIONAL {{ ?culturalObject base_url:owner ?owner . }}
        }}
        """
        
        print("SPARQL Query:")
        print(query)

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = sparql.query().convert()
            print("Query Results:")
            print(results)
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()

        objects = []
        for result in results["results"]["bindings"]:
            obj = {
                "objectId": result["objectId"]["value"],
                "title": result.get("title", {}).get("value", ""),
                "date": result.get("date", {}).get("value", ""),
                "place": result.get("place", {}).get("value", ""),
                "owner": result.get("owner", {}).get("value", "")
            }
            objects.append(obj)

        df = pd.DataFrame(objects)    
