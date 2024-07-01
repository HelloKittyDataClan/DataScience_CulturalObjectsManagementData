from typing import Optional #serve per indicare che un certo valore può essere presente o assente,
import json
import csv

#Imports fo relational db
from pandas import Series
import json
from typing import Any, Dict, List
from sqlite3 import connect

#imports for graph db 
from rdflib import Namespace 
from rdflib.namespace import FOAF
from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.plugins.sparql import prepareQuery
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib.request

#imports for both
import pandas as pd
from pandas import DataFrame  #per la creazione del dataframe 

#JSON upload and read 
#Specify the path to your JSON file
json_path = "1_CLASSES/process.json"

#Open and read the JSON file
with open(json_path, 'r') as file:
    json_data = file.read()

#Parse the JSON data into a Python object
data = json.loads(json_data)

#Utilize the data for UML purposes
#Print the data to inspect its structure

#print(data) --> controller

class IdentifiableEntity(object): #identifichiamo l'ID
    def __init__(self, id: str):
        if not isinstance(id, str): #se l'ID non è una stringa
            raise ValueError("ID must be a string for the IdentifiableEntity")
        self.id = id #deve essere necessariamente una stringa cosi che l'ID sia valido sempre

    def get_id(self):
        return self.id

#___________________________CSV_________________________

class CulturalObject(IdentifiableEntity):
    def __init__(self, id:str, title:str, owner:str, place:str, date:str= None,authors:list=None): #vado a definire title, date, owner, place, Author  del mio csv 
        super().__init__(id)  #cosi facendo vado a richiamare l'ID della classe IdentifiableEntity
        if not isinstance(title, str):
            raise ValueError("Title must be a string for the CulturalObject")
        if not isinstance(owner, str):
            raise ValueError("Owner must be a string for the CulturalObject")
        if not isinstance(place, str):
            raise ValueError("Place must be a string for the CulturalObject")
        if date is not None and not isinstance(date, str):
            raise ValueError("Date must be a string or None for the CulturalObject")
        if authors is not None:
            if not all(isinstance(author, Person) for author in authors):
                raise ValueError("Authors must be instances of Person or None for the CulturalObject")    # garantisce che gli autori forniti per il nostro oggetto culturale siano parte della classe Person
        
        self.title=title
        self.date=date
        self.owner=owner
        self.place=place
        self.authors=[]    #lista vuota assegnata all'attributo authors 

        if authors:
            if isinstance(authors, Person):
                self.authors.append(authors)
            elif isinstance(authors, list):
                self.authors.extend(authors)       #si occupa di aggiungere gli autori forniti all'oggetto culturale CulturalObject, gestendo sia il caso in cui sia fornito un singolo autore come istanza di Person, sia il caso in cui siano forniti più autori come lista di istanze di Person.
   
    def getTitle(self):
        return self.title

    def getDate(self):
        if self.date:
           return self.date
        return None
        
    def getOwner(self):
        return self.owner

    def getPlace(self):
        return self.place

    def getAuthors(self):
        return self.authors


#definiamo le sottoclassi relative alla classe Cultrual Object   
class NauticalChart(CulturalObject):
    pass

class ManuscriptPlate(CulturalObject):
    pass

class ManuscriptVolume(CulturalObject):
    pass

class PrintedVolume(CulturalObject):
    pass

class PrintedMaterial(CulturalObject):
    pass

class Herbarium(CulturalObject):
    pass

class Specimen(CulturalObject):
    pass

class Painting(CulturalObject):
    pass

class Model(CulturalObject):
    pass

class Map(CulturalObject):
    pass



#____________________ JSON______________________

#Creation of class Person that refers to CulturalObject
class Person(IdentifiableEntity):
    def __init__(self, name: str): #define parameter name
        super().__init__(id)
        if not isinstance(name, str):
            raise ValueError("Name must be a string for the Person")
        self.name = name
    
    def getName(self):
        return self.name

#Creation of class Activity that refers to CulturalObject
class Activity():                               
    def _init_(self, institute: str, person: str= None, tool: str|set[str]|None = None, start: str = None, end: str = None): 
        super().__init__()  # Initialize (replace with appropriate values)
        if not isinstance(institute, str):
            raise ValueError("Institute must be a string for the Activity")
        if person is not None and not isinstance(person, str):
            raise ValueError("Person must be a string or None for the Activity")
        if not isinstance(tool, str, set[str]):
            raise ValueError("Tool must be a string or a set of strings for the Activity")
        if start is not None and not isinstance(start, str):
            raise ValueError("Start Date must be a string or None for the Activity")
        if end is not None and not isinstance(start, str):
            raise ValueError("End Date must be a string or None for the Activity")
        self.institute = institute
        self.person = person
        self.tool = tool         
        self.start = start
        self.end = end
        
    def getResponsibleInstitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        return self.person

    def getTools(self):
        return self.tool
    
    def getStartDate(self):
        return self.start 
    
    def getEndDate(self):
        return self.end
    
    def refersTo(self, CulturalObject):     #---->>>non si riferisce a nessun oggetto e non ti ritorna nulla, TI DEVE RITORNARE CULTURAL OBJECT!!
        if isinstance(CulturalObject):
            self.title.append(CulturalObject)
        else:
            raise ValueError("Invalid object type provided")

#Subclass of Activity just with technique parameter

class Acquisition(Activity):
    def _init_(self, technique: str):   
        super().__init__() 
        if not isinstance(technique, str):
            raise ValueError("Acquisition.technique must be a string")
        self.technique = technique
        
    def getTechnique(self):
        return self.technique

#Subclasses without defined parameters
class Processing(Activity):
    pass
        
class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass


#__________________________________________#

#basic Handlers Classes 
class Handler:  # Chiara
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass


class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl: str = ""):
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str) -> DataFrame:
        pass


#_______________CSV PART__________#

class MetadataUploadHandler(UploadHandler):  # Chiara
    def __init__(self):
        self.dbPathOrUrl = ""

    def pushDataToDb(self, path) -> bool:

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
            # Rimuoviamo i duplicati della colonna id, mantenendo la prima istanza
            venus.drop_duplicates(subset=["Id"], keep="first", inplace=True, ignore_index=True)

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
            name = URIRef(FOAF + "name")  # URI di FOAF http://xmlns.com/foaf/0.1/

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
                        my_graph.add((URIRef(base_url +"culturalobject-" + object_id), relAuthor, author_uri))  # MODIFICA mancava cultural object come parte del predicato

            # Store RDF data in SPARQL endpoint
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl() + "sparql"
            store.open((endpoint, endpoint))  # First parameter reading data, second write database

            for triple in my_graph.triples((None, None, None)):
                store.add(triple)

            store.close()

#Bea
class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl: str = ""):
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str) -> DataFrame:
        pass

class MetadataQueryHandler(QueryHandler):
    def __init__(self, db_url):
        super().__init__()
        self.dbPathOrUrl = db_url

    def execute_sparql_query(self, query):
        sparql = SPARQLWrapper(self.dbPathOrUrl + "sparql")
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)
        ret = sparql.queryAndConvert()
        df_columns = ret["head"]["vars"]
        
        # Collect row data in a list
        rows = []
        for row in ret["results"]["bindings"]:
            row_dict = {column: row[column]["value"] if column in row else None for column in df_columns}
            rows.append(row_dict)
        
        # Create DataFrame from the list of rows
        df = pd.DataFrame(rows, columns=df_columns)
        return df
        
    


    def getById(self, id):
        person_query_str = """
            SELECT DISTINCT ?uri ?name ?id 
            WHERE {
                ?uri <http://schema.org/identifier> "%s" ;    #ostituire con ULAN o VIAF 
                     <http://xmlns.com/foaf/0.1/name> ?name ;
                     <http://schema.org/identifier> ?id .
                ?object <http://schema.org/author> ?uri .
            }
        """ % id
        
        object_query_str = """
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE {
                ?object <http://schema.org/identifier> "%" . #sostutuire con id autore numero da 1 a 35
                ?object <http://schema.org/identifier> ?id  .
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
                ?object <http://schema.org/title> ?title .
                ?object <http://github.com/HelloKittyDataClan/DSexam/owner> ?owner .
                ?object <http://schema.org/itemLocation> ?place .
                 
                OPTIONAL {
                    ?object <http://schema.org/dateCreated> ?date .
                    ?object <http://schema.org/author> ?author .
                    ?author <http://xmlns.com/foaf/0.1/name> ?author_name .
                    ?author <http://schema.org/identifier> ?author_id .
                }
            }

        """ % id

        # Prepare SPARQL queries
        person_query = prepareQuery(person_query_str, initNs={"schema": URIRef("http://schema.org/"), "foaf": URIRef("http://xmlns.com/foaf/0.1/")})
        object_query = prepareQuery(object_query_str, initNs={"schema": URIRef("http://schema.org/"), "base_url": URIRef("http://github.com/HelloKittyDataClan/DSexam/")})
        
        # Execute SPARQL queries
        person_df = self.execute_sparql_query(person_query)
        # Execute SPARQL queries
        person_df = self.execute_sparql_query(person_query)
        object_df = self.execute_sparql_query(object_query)

        # Check if objects exist and return accordingly
        if len(object_df) > 0:
            return object_df
        else:
            return person_df
    
    def getAllPeople(self):
        query = """
        PREFIX FOAF: <http://xmlns.com/foaf/0.1/>
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?uri ?author_name ?author_id
        WHERE {
            ?uri a FOAF:Person ;
                 schema:identifier ?author_id ;
                 FOAF:name ?author_name .
        }
        """
        results = self.execute_sparql_query(query)
        rows = []
        for result in results["results"]["bindings"]:
            row = {
                "uri": result["uri"]["value"],
                "author_name": result["author_name"]["value"],
                "author_id": result["author_id"]["value"]
            }
            rows.append(row)
    
        df = pd.DataFrame(rows)
        return df
    
    
    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX base_url: <http://github.com/HelloKittyDataClan/DSexam/>
        PREFIX db: <https://dbpedia.org/property/>

        SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?authorName ?authorID
        WHERE {
            ?object a ?type ;
                    schema:identifier ?id ;
                    schema:title ?title ;
                    base_url:owner ?owner ;
                    schema:itemLocation ?place .
                
            OPTIONAL { ?object schema:dateCreated ?date }
            OPTIONAL {
                ?object schema:author ?author .
                ?author foaf:name ?authorName ;
                schema:identifier ?authorID .
            }
        
            FILTER (?type IN (
                base_url:NauticalChart,
                base_url:ManuscriptPlate,
                base_url:ManuscriptVolume,
                base_url:PrintedVolume,
                base_url:PrintedMaterial,
                db:Herbarium,
                base_url:Specimen,
                db:Painting,
                db:Model,
                db:Map
            ))
        }
        """
    
        results = self.execute_sparql_query(query)
    
        objects = []
        for result in results["results"]["bindings"]:
            obj = {}
            obj["object"] = result["object"]["value"] if "object" in result else None
            obj["id"] = result["id"]["value"] if "id" in result else None
            obj["type"] = result["type"]["value"] if "type" in result else None
            obj["title"] = result["title"]["value"] if "title" in result else None
            obj["date"] = result["date"]["value"] if "date" in result else None
            obj["owner"] = result["owner"]["value"] if "owner" in result else None
            obj["place"] = result["place"]["value"] if "place" in result else None
            obj["authorName"] = result["authorName"]["value"] if "authorName" in result else None
            obj["authorID"] = result["authorID"]["value"] if "authorID" in result else None
            objects.append(obj)

        return pd.DataFrame(objects)



    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT DISTINCT ?authorName ?authorID 
        WHERE {{
          ?object schema:identifier "{object_id}" ;
                  schema:author ?entity .

          ?author schema:author ?entity ;
                  foaf:name ?authorName ;
                  schema:identifier ?authorID .
        }}
        """

        
        sparql = SPARQLWrapper(self.dbPathOrUrl + "sparql")
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        
        columns = ['authorName', 'authorID', 'author']
        data = []
        for result in results['results']['bindings']:
            author_name = result.get('authorName', {}).get('value', None)
            author_id = result.get('authorID', {}).get('value', None)
            author_uri = result.get('author', {}).get('value', None)
            data.append([author_name, author_id, author_uri])

        df = pd.DataFrame(data, columns=columns)

       
        df.drop_duplicates(subset=['authorName', 'authorID'], keep='first', inplace=True)

        return df
    
    def getCulturalHeritageObjectsAuthoredBy(self, personid: str) -> pd.DataFrame:
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX base_url: <http://github.com/HelloKittyDataClan/DSexam/>
        PREFIX db: <https://dbpedia.org/property/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?authorName ?authorID
        WHERE {{
            ?object a ?type ;
                    schema:identifier ?id ;
                    schema:title ?title ;
                    base_url:owner ?owner ;
                    schema:itemLocation ?place ;
                    schema:author ?author .

            ?author foaf:name ?authorName ;
                    schema:identifier ?authorID ;
                    schema:identifier "{personid}" .

            OPTIONAL {{ ?object schema:dateCreated ?date }}

            FILTER (?type IN (
                base_url:NauticalChart,
                base_url:ManuscriptPlate,
                base_url:ManuscriptVolume,
                base_url:PrintedVolume,
                base_url:PrintedMaterial,
                db:Herbarium,
                base_url:Specimen,
                db:Painting,
                db:Model,
                db:Map
            ))
        }}
        """

       
        sparql = SPARQLWrapper(self.dbPathOrUrl + "sparql")
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = sparql.query().convert()
        except Exception as e:
            print(f"Errore nella query SPARQL: {e}")
            return pd.DataFrame()  # Ritorna un DataFrame vuoto in caso di errore

        
        objects = []
        for result in results.get("results", {}).get("bindings", []):
            obj = {
                "object": result.get("object", {}).get("value", None),
                "id": result.get("id", {}).get("value", None),
                "type": result.get("type", {}).get("value", None),
                "title": result.get("title", {}).get("value", None),
                "date": result.get("date", {}).get("value", None),
                "owner": result.get("owner", {}).get("value", None),
                "place": result.get("place", {}).get("value", None),
                "authorName": result.get("authorName", {}).get("value", None),
                "authorID": result.get("authorID", {}).get("value", None),
            }
            objects.append(obj)

        return pd.DataFrame(objects)



 
 #__________JSON____________________#

  class ProcessDataUploadHandler(UploadHandler):  #Cata
    def __init__(self):
        super().__init__()

    #Create data frame with the objects ID beacuse activities refers to. (Data Frame is a function that allows us to create kind of tables with pandas)
    def pushDataToDbObject(self, file_path: str) -> pd.DataFrame:
        cultural_objects = pd.read_csv(file_path, keep_default_na=False,
                            dtype={
                                "Id": "string", 
                                "Type": "string", 
                                "Title": "string",
                                "Date": "string", 
                                "Author": "string", 
                                "Owner": "string",
                                "Place": "string"})
        
        objects_ids = cultural_objects[["Id"]]
        objects_internal_ids = ["object-" + str(idx) for idx in range(len(objects_ids))]
        objects_ids.insert(0, "objectId", pd.Series(objects_internal_ids, dtype="string"))
        
        objects_ids_df = pd.DataFrame(objects_ids)
        objects_ids_df.to_csv('objects_ids.csv', index=False)
        
        return objects_ids_df

    #Create a function to create 5 different tables
    def pushDataToDbActivities(self, file_path: str, field_name: str) -> pd.DataFrame:
        # Load JSON data from file
        with open(file_path, 'r') as file:
            df_activity = json.load(file)
        
        table_data: List[Dict[str, Any]] = []
        
        for item in df_activity:
            if field_name in item:
                field_entry = item[field_name]
                field_entry['object id'] = item['object id']
                table_data.append(field_entry)

        # Convert the list of dictionaries to a DataFrame
        df_activities = pd.DataFrame(table_data)
        
        # Convert lists in 'tool' column to comma-separated strings because lists are not readed
        if 'tool' in df_activities.columns:
            df_activities['tool'] = df_activities['tool'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
            
        return df_activities

    #Create internalId for each one (important for queries)
    def addInternalIds(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        # Create internal IDs
        internal_ids = [f"{field_name}-{idx}" for idx in range(len(df))]
        # Add the internal IDs as a new column
        df.insert(0, "internalId", Series(internal_ids, dtype="string"))
        
        return df
    
    # Function to join activities with objects based on object id (to guarantee that the activities are connected with the objects)
    def joinActivitiesWithObjects(self, df_activities: pd.DataFrame, objects_ids_df: pd.DataFrame, left_on: str, right_on: str) -> pd.DataFrame:
        return pd.merge(df_activities, objects_ids_df, left_on=left_on, right_on=right_on, how="left")

    #Replace object id with objectId (internal id of objects). Two cases with the row technique included (activities) or without this data
    def extractAndRenameColumns(self, df: pd.DataFrame, include_technique: bool = False) -> pd.DataFrame:
        columns = ["internalId", "object id", "responsible institute", "responsible person", "tool", "start date", "end date"]
        if include_technique:
            columns.insert(4, "technique")  # Insert 'technique' column before 'tool'
        
        identifiers = df[columns]
        identifiers = identifiers.rename(columns={"object id": "objectId"})
        return identifiers
        
    #Create individual DataFrame tables calling the pushDataToDbActivities, internal ID, etc.
    def createTablesActivity(self, activities_file_path: str, objects_file_path: str):
        #Create individual DataFrames
        acquisition_df = self.pushDataToDbActivities(activities_file_path, 'acquisition')
        processing_df = self.pushDataToDbActivities(activities_file_path, 'processing')
        modelling_df = self.pushDataToDbActivities(activities_file_path, 'modelling')
        optimising_df = self.pushDataToDbActivities(activities_file_path, 'optimising')
        exporting_df = self.pushDataToDbActivities(activities_file_path, 'exporting')

        #Add internal IDs to each DataFrame
        acquisition_df = self.addInternalIds(acquisition_df, 'acquisition')
        processing_df = self.addInternalIds(processing_df, 'processing')
        modelling_df = self.addInternalIds(modelling_df, 'modelling')
        optimising_df = self.addInternalIds(optimising_df, 'optimising')
        exporting_df = self.addInternalIds(exporting_df, 'exporting')

        #Load object IDs
        objects_ids_df = self.pushDataToDbObject(objects_file_path)

        #Join activity DataFrames with objects DataFrame
        acquisition_joined = self.joinActivitiesWithObjects(acquisition_df, objects_ids_df, "object id", "objectId")
        processing_joined = self.joinActivitiesWithObjects(processing_df, objects_ids_df, "object id", "objectId")
        modelling_joined = self.joinActivitiesWithObjects(modelling_df, objects_ids_df, "object id", "objectId")
        optimising_joined = self.joinActivitiesWithObjects(optimising_df, objects_ids_df, "object id", "objectId")
        exporting_joined = self.joinActivitiesWithObjects(exporting_df, objects_ids_df, "object id", "objectId")

        #Extract and rename columns, including 'technique' for acquisition
        acquisition_final_db = self.extractAndRenameColumns(acquisition_joined, include_technique=True)
        processing_final_db = self.extractAndRenameColumns(processing_joined)
        modelling_final_db = self.extractAndRenameColumns(modelling_joined)
        optimising_final_db = self.extractAndRenameColumns(optimising_joined)
        exporting_final_db = self.extractAndRenameColumns(exporting_joined)
        
        #Save to SQLite database in the file
        with connect("activities.db") as con:
            objects_ids_df.to_sql("ObjectId", con, if_exists="replace", index=False)
            acquisition_final_db.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_final_db.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_final_db.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_final_db.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_final_db.to_sql("Exporting", con, if_exists="replace", index=False)
        
        #Printing the tables in the console (not necessary)
        print("\nAcquisition DB:")
        print(acquisition_final_db)
        print("\nProcessing DB:")
        print(processing_final_db)
        print("\nModelling DB:")
        print(modelling_final_db)
        print("\nOptimising DB:")
        print(optimising_final_db)
        print("\nExporting DB:")
        print(exporting_final_db)

#How to implement the code (example):
process_upload = ProcessDataUploadHandler()
process_upload.createTablesActivity('process.json', 'meta.csv')


#_____________QUERIES_____________________________

class QueryHandler(Handler): #Elena
    def __init__(self):
        super().__init__()

    def getById(self, ID: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query = f"SELECT * FROM ObjectId WHERE Id = '{ID}'"
                result = pd.read_sql(query, con)
                return result
        except Exception as e:
            print(f"An error occurred: {e}")

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllActivities(self):
        try:
            # Modify the partialName parameter to include "wildcard characters" for partial matching
            # nell'input se inserisco anche solo un risultato parziale mi compare comunque
            with connect(self.getDbPathOrUrl()) as con:
                # Connect to the database #modific con la parte di catalina
                query = """
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    UNION 
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", NULL AS objectId, NULL AS technique
                    FROM processing
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", NULL AS objectId, NULL AS technique
                    FROM modelling
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                """
                result = pd.read_sql(query, con)
                return result
        except Exception as e:
            print(f"An error occurred: {e}")

    def getActivitiesByResponsibleInstitution(self, partialName: str): 
        # questo è un metodo
        #partialName lo da peroni come parametro?, è la stringa di input
        try:
            # Modify the partialName parameter to include "wildcard characters" for partial matching
            # nell'input se inserisco anche solo un risultato parziale mi compare comunque

            partial_name = f"%{partialName}%"
            
            # Connect to the database #modific con la parte di catalina
            with connect(self.getDbPathOrUrl()) as con:
                # Define the SQL query with placeholders for parameters, quindi seleziono tutte le colonne
                # con il FROM indico da che ? .... tabella
                # con il where da che colonna
                query2 = """
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM processing
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM modelling
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                WHERE "responsible institute" LIKE ?
                """
                # Execute the query with the provided parameters
                query2_table = pd.read_sql(query2, con, params=[partial_name]*5)
                return query2_table
        except Exception as e:
            print("An error occurred:", e)
            return None

    def getActivitiesByResponsiblePerson(self, partialName: str):
            try:
                partial_name = f"%{partialName}%"
                with connect(self.getDbPathOrUrl()) as con:
                    query3 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE "responsible person" LIKE ?
                    """
                    query3_table = pd.read_sql(query3, con, params= [partial_name]*5)
                    return query3_table
            except Exception as e:
                print("An error occurred:", e)
                
    def getActivitiesUsingTool(self, partialName: str):
            try:
                partial_name= f"%{partialName}%"
                with connect(self.getDbPathOrUrl()) as con:
                    query4 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE tool LIKE ?
                    """
                    query4_table = pd.read_sql(query4, con, params=[partial_name]*5)
                    return query4_table
            except Exception as e:
                print("An error occurred:", e)
                
    def getActivitiesStartedAfter (self, date: str):
            try:
                date=f"%{date}%"
                with connect(self.getDbPathOrUrl()) as con:
                    query5 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE "start date" LIKE ?
                    UNION
                    -- con l'union faccio 1 +1 con
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE "start date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE "start date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE "start date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE "start date" LIKE ?
                    """
                    query5_table = pd.read_sql(query5, con, params=[date]*5)
                    return query5_table
            except Exception as e:
                print ("An error occured:", e)

    def getActivitiesEndedBefore(self, date: str):
            try:
                date=f"%{date}%"
                with connect (self.getDbPathOrUrl()) as con:
                    query6 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE "end date" LIKE ?
                    UNION  
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE "end date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE "end date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE "end date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE "end date" LIKE ?
                    """
                    query6_table = pd.read_sql(query6, con, params=[date]*5)
                    return query6_table
            except Exception as e:
                print ("An error occured:", e) 

    def getAcquisitionsByTechnique(self, partialName: str):
        try:
            partial_name = f"%{partialName}%"
            
            with connect(self.getDbPathOrUrl()) as con:
                query = "SELECT * FROM acquisition WHERE technique LIKE ?"
                query_table = pd.read_sql(query, con, params=[partial_name])
                return query_table
        except Exception as e:
            print("An error occurred:", e)
            return None

        
#__________________________ TESTS ___________________________

#Calling all the functions:
process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("activities.db")

#Calling to get all activities
all_activities = process_query.getAllActivities()
print("All Activities:")
print(all_activities)

#Calling to get activities by responsible institution
partial_name_institution = "Council"
activities_by_institution = process_query.getActivitiesByResponsibleInstitution(partial_name_institution)
print(f"Activities by institution containing '{partial_name_institution}':")
print(activities_by_institution)

#Calling to get activities by a responsible person
partial_name_person = "Jane Doe"
activities_by_person = process_query.getActivitiesByResponsiblePerson(partial_name_person)
print(f"Activities by responsible person '{partial_name_person}':")
print(activities_by_person)

#Calling to get activities by tool
partial_name_tool = "Nikon"
activities_by_tool = process_query.getActivitiesUsingTool(partial_name_tool)
print(f"Activities by responsible tool '{partial_name_tool}':")
print(activities_by_tool)

#Calling to get activities started after a specific date
start_date = "2023-04-17"
activities_started_after = process_query.getActivitiesStartedAfter(start_date)
print(f"Activities started after '{start_date}':")
print(activities_started_after)

#Calling to get activities ended after a specific date
end_date = "2023-03-04"
activities_end_after = process_query.getActivitiesEndedBefore(end_date)
print(f"Activities ended after '{end_date}':")
print(activities_end_after)

#Calling to get activities by technique
technique_partial_name = "Structured-light 3D scanner"
acquisitions_by_technique = process_query.getAcquisitionsByTechnique(technique_partial_name)
print(f"Acquisitions with technique containing '{technique_partial_name}':")
print(acquisitions_by_technique)
