from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.namespace import FOAF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
from datetime import datetime
import pandas as pd
import csv
import json
import re

from sparql_dataframe import get
from typing import Optional, List, Any, Dict
from pandas import Series
from sqlite3 import connect
from pprint import pp
from os import sep
process = "data" + sep + "process.json"


class IdentifiableEntity(object): 
    def __init__(self, id: str):
        self.id = id 

    def getId(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, id: str, name: str):
        super().__init__(id)
        self.name = name
    
    def getName(self):
        return self.name


#___________________________CSV_________________________

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, owner: str, place: str, date:str = None, authors: Person|list[Person]|None=None):
        super().__init__(id)
        self.title = title
        self.owner = owner
        self.place = place
        self.date = date
        self.authors = list() #should return a list? 

        if type(authors) == Person:
            self.authors.append(Person)
        elif type(authors) == list:
            self.authors = authors

    def getTitle(self) -> str:
        return self.title

    def getDate(self) -> Optional[str]:
        if self.date:
            return self.date
        return None
        
    def getOwner(self) -> str:
        return self.owner

    def getPlace(self) -> str:
        return self.place

    def hasAuthors(self) -> list[Person]:
        return self.authors
    

class NauticalChart(CulturalHeritageObject):
    pass

class ManuscriptPlate(CulturalHeritageObject):
    pass

class ManuscriptVolume(CulturalHeritageObject):
    pass

class PrintedVolume(CulturalHeritageObject):
    pass

class PrintedMaterial(CulturalHeritageObject):
    pass

class Herbarium(CulturalHeritageObject):
    pass

class Specimen(CulturalHeritageObject):
    pass

class Painting(CulturalHeritageObject):
    pass

class Model(CulturalHeritageObject):
    pass

class Map(CulturalHeritageObject):
    pass



#____________________ JSON______________________


class Activity(object):                               
    def __init__(self, object: CulturalHeritageObject, institute: str, person: str, start: str, end: str, tool: str|list[str]):
        self.object = object
        self.institute = institute
        self.person = person     
        self.start = start
        self.end = end
        self.tool = []

        if type(tool) == str:
            self.tool.append(tool)
        elif type(tool) == list:
            self.tool = tool
    
    '''def __str__(self):
        tools_str = ", ".join(self.tool)
        return (f"Activity: {self.__class__.__name__}, Institute: {self.institute}, "
                f"Person: {self.person}, Start: {self.start}, End: {self.end}, Tool: {tools_str}")'''

    def getResponsibleInstitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        if self.person:
            return self.person 
        return None
    
    def getTools(self): 
        return self.tool
    
    def getStartDate(self):
        if self.start:
            return self.start
        return None

    def getEndDate(self):
        if self.start:
            return self.end
        return None
        
    def refersTo(self):
        return self.object

#Subclass of Activity just with technique parameter

class Acquisition(Activity):
    def __init__(self, object: CulturalHeritageObject, institute: str, technique: str, person: str, start: str, end: str, tool: str|list[str]):
        # Make sure to match the constructor of the base Activity class
        super().__init__(object, institute, person, start, end, tool)  # Pass in the correct parameters
        
        self.technique = technique  # 'technique' is specific to 'Acquisition'

    '''def __str__(self):
        # Extend the base class string representation
        return f"{super().__str__()}, Technique: {self.technique}"'''

    def getTechnique(self):
        return self.technique
    

class Processing(Activity):
    '''def __str__(self):
        return super().__str__()'''
    pass


class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass

#_______________HANDLERS_____________________


class Handler(object):  # chiara
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

class MetadataUploadHandler(UploadHandler):  # chiara
    def __init__(self):  
        super().__init__()

    def pushDataToDb(self, path) -> bool:
            my_graph = Graph()
            # Read the data from the csv file and store them into a dataframe
            venus = pd.read_csv(path,
                                keep_default_na=False,
                                dtype={
                                    "Id": str,
                                    "Type": str,
                                    "Title": str,
                                    "Date": str,
                                    "Author": str,
                                    "Owner": str,
                                    "Place": str,
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
                loc_id = "culturalobject-" + str(row["Id"])
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

                if row["Id"] != "":
                        my_graph.add((subj, id, Literal(str(row["Id"]))))
                # Assign title
                if row["Title"] != "":
                    title_value = row["Title"].strip()
                    my_graph.add((subj, title, Literal(str(title_value)))) 
                # Assign date
                if row["Date"] != "":
                    my_graph.add((subj, date, Literal(str(row["Date"]))))
                # Assign owner
                if row["Owner"] != "":
                    my_graph.add((subj, owner, Literal(str(row["Owner"]))))
                # Assign place
                if row["Place"] != "":
                    my_graph.add((subj, place, Literal(str(row["Place"]))))

                if row["Author"] != "":
                    author_list = row["Author"].split(";")  # Separa gli autori
                    
                    for author in author_list:
                        if "(" in author and ")" in author:  # Controlla se ci sono parentesi per ID
                            split_index = author.index("(")
                            author_name = author[:split_index - 1].strip()  # Estrai il nome dell'autore
                            author_id = author[split_index + 1:-1].strip()  # Estrai l'ID dell'autore
                            
                            # Crea l'URI relativo alla persona (autore)
                            related_person = URIRef(base_url + "Person/" + author_id)
                            
                            # Aggiungi le triple RDF per collegare l'oggetto principale (subj) con l'autore
                            my_graph.add((subj, relAuthor, related_person))  # Oggetto principale -> Autore (relazione)
                            # Aggiungi il nome e l'ID dell'autore all'URI della persona
                            my_graph.add((related_person, name, Literal(author_name))) 
                            my_graph.add((related_person, id, Literal(author_id))) 
                            

            # Store RDF data in SPARQL endpoint
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl() + "sparql"
            store.open((endpoint, endpoint))  # First parameter reading data, second write database

            for triple in my_graph.triples((None, None, None)):
                store.add(triple)

            store.close()

            # Verificare se il grafo è stato caricato 
            query_graph = """
            SELECT ?s ?p ?o
            WHERE {
                ?s ?p ?o .
            }
            """
            result_dataset = get(endpoint, query_graph, True)

            num_triples_blazegraph = len(result_dataset)
            num_triples_local = len(my_graph)

            pos = num_triples_blazegraph == num_triples_local
            if pos:
                print("I dati sono stati caricati con successo su Blazegraph.")
                return True
            else:
                print("Caricamento dei dati su Blazegraph non riuscito.")
                return False
        
        



#_____________________RELATIONAL DATA BASE____________________________


class ProcessDataUploadHandler(UploadHandler):  #Cata
    def __init__(self):
        super().__init__()


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

    #Replace object id with objectId (internal id of objects). Two cases with the row technique included (activities) or without this data
    def extractAndRenameColumns(self, df: pd.DataFrame, include_technique: bool = False) -> pd.DataFrame:
        columns = ["internalId", "object id", "responsible institute", "responsible person", "tool", "start date", "end date"]
        if include_technique:
            columns.insert(4, "technique")  # Insert 'technique' column before 'tool'
        
        identifiers = df[columns]
        identifiers = identifiers.rename(columns={"object id": "objectId"})
        return identifiers
        
    #Create individual DataFrame tables calling the pushDataToDbActivities, internal ID, etc.
    def pushDataToDb(self, activities_file_path: str):
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
        
        #Extract and rename columns, including 'technique' for acquisition
        acquisition_final_db = self.extractAndRenameColumns(acquisition_df, include_technique=True)
        processing_final_db = self.extractAndRenameColumns(processing_df)
        modelling_final_db = self.extractAndRenameColumns(modelling_df)
        optimising_final_db = self.extractAndRenameColumns(optimising_df)
        exporting_final_db = self.extractAndRenameColumns(exporting_df)
        
        #Save to SQLite database in the file
        with connect("relational.db") as con:
            acquisition_final_db.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_final_db.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_final_db.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_final_db.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_final_db.to_sql("Exporting", con, if_exists="replace", index=False)
        
        return True



class QueryHandler(Handler):
    def __init__(self,):  
        super().__init__()
    

    def getById(self, id: str) -> pd.DataFrame:     #bea
        id = str(id)
        
        if self.getDbPathOrUrl().startswith("http"):
            db_address = self.getDbPathOrUrl()
        else:
            return pd.DataFrame() 
        
        # Se l'URL è valido, aggiungi "sparql" all'endpoint
        endpoint = db_address + "sparql" if db_address.endswith("/") else db_address + "/sparql"


        # Se l'ID è numerico
        if id.isdigit():
            query = """
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE {
                ?object <http://schema.org/identifier> "%s" .
                ?object <http://schema.org/identifier> ?id .
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
                ?object <http://schema.org/title> ?title .
                ?object <http://github.com/HelloKittyDataClan/DSexam/owner> ?owner .
                ?object <http://schema.org/itemLocation> ?place .
            
                OPTIONAL {?object <http://schema.org/dateCreated> ?date .}
                OPTIONAL {?object <http://schema.org/author> ?author .}
                OPTIONAL {?author <http://xmlns.com/foaf/0.1/name> ?author_name .}
                OPTIONAL {?author <http://schema.org/identifier> ?author_id .}
            }
            """ % id
        else:
            # Se l'ID non è numerico
            query = """
            SELECT DISTINCT ?uri ?author_name ?author_id 
            WHERE {
                ?uri <http://schema.org/identifier> "%s" ;
                    <http://xmlns.com/foaf/0.1/name> ?author_name ;
                    <http://schema.org/identifier> ?author_id  .
                ?object <http://schema.org/author> ?uri .
            }
            """ % id

        results = get(endpoint, query, True) 
        return results
    

class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    def getAllPeople(self):         #chiara
        query = """
        PREFIX FOAF: <http://xmlns.com/foaf/0.1/>
        PREFIX schema: <http://schema.org/>


        SELECT DISTINCT ?id_auth ?name_auth
        WHERE {
            ?c_obj schema:author ?auth .
            ?auth schema:identifier ?id_auth ;
                FOAF:name ?name_auth .
                    }
        """
        results = get(self.dbPathOrUrl + "sparql", query, True)
        return results

    
    def getAllCulturalHeritageObjects(self):        #bea
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
        results = get(self.dbPathOrUrl + "sparql", query, True)
        return results       
    

    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:          #chiara
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT DISTINCT ?authorName ?authorID 
        WHERE {{
        ?object schema:identifier "{object_id}" ;
                schema:author ?uri .

        ?uri schema:identifier ?authorID ;
            foaf:name ?authorName .
        }} 
        """
        results = get(self.dbPathOrUrl + "sparql",query, True)
        return results
    
    
    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> pd.DataFrame:          #bea
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
                    schema:identifier "{personId}" .

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
        results = get(self.dbPathOrUrl + "sparql",query, True)
        return results


#_____________QUERIES_____________________________#elena

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllActivities(self):
        with connect(self.getDbPathOrUrl()) as con:
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            union_list = [
                pd.read_sql(f"SELECT * FROM {table}", con)
                for table in tables
            ]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union.fillna("")
    
    def getActivitiesByResponsibleInstitution(self, partialName: str):
        with connect(self.getDbPathOrUrl()) as con:
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            union_list = [
                pd.read_sql(
                    f'SELECT * FROM {table} WHERE "responsible institute" LIKE ?',
                    con,
                    params=(f"%{partialName}%",)
                )
                for table in tables
            ]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union.fillna("")
    
    def getActivitiesByResponsiblePerson(self, partialName: str):
        with connect(self.getDbPathOrUrl()) as con:
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            union_list = [
                pd.read_sql(
                    f'SELECT * FROM {table} WHERE "responsible person" LIKE ?',
                    con,
                    params=(f"%{partialName}%",)
                )
                for table in tables
            ]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union.fillna("")

                
    def getActivitiesUsingTool(self, partialName: str):
        with connect(self.getDbPathOrUrl()) as con:
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            union_list = [
                pd.read_sql(
                    f'SELECT * FROM {table} WHERE "tool" LIKE ?',
                    con,
                    params=(f"%{partialName}%",)
                )
                for table in tables
            ]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union.fillna("")
    
    def getActivitiesStartedAfter(self, date: str):
        with connect(self.getDbPathOrUrl()) as con:
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            union_list = [
                pd.read_sql(f'SELECT * FROM {table} WHERE "start date" >= ?', con, params=(date,))
                for table in tables
            ]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union.fillna("")
        
    def getActivitiesEndedBefore(self, date: str):
        with connect(self.getDbPathOrUrl()) as con:
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            union_list = [
                pd.read_sql(
                    f'SELECT * FROM {table} WHERE "end date" <= ? AND NOT "end date" = ""',
                    con,
                    params=(date,)
                )
                for table in tables
            ]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union

    def getAcquisitionsByTechnique(self, partialName: str):
        with connect(self.getDbPathOrUrl()) as con:
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            union_list = [
                pd.read_sql(
                    f'SELECT * FROM {table} WHERE "technique" LIKE ?',
                    con,
                    params=(f"%{partialName}%",)
                )
                for table in tables
            ]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union.fillna("")

'''
# Instantiate the query handler
process_query = ProcessDataQueryHandler()

# Set the database path or URL
process_query.setDbPathOrUrl("relational.db")

# Call the method and store the result
getAcquisitionsByTechnique = process_query.getAcquisitionsByTechnique("Photogrammetry")

pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
# Print the result
print("Result from getActivitiesStartedAfter:")
print(getAcquisitionsByTechnique)

'''

#BasicMashup

class BasicMashup(object):
    def __init__(self) -> None:
        self.metadataQuery = list()
        self.processQuery = list()
    
    def cleanMetadataHandlers(self) -> bool:    #chiara
        self.metadataQuery = []
        return True

    def cleanProcessHandlers(self) -> bool:      #cata
        self.processQuery = [] 
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:     #bea
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler:ProcessDataQueryHandler) -> bool:   #elena
        if not isinstance(handler, ProcessDataQueryHandler):
        # check if handler is an istance of processdataqueryhandler (prevention)
            return False
        else:
            self.processQuery.append(handler)  # Adds a process handler to the list
            return True
        
    def getEntityById(self, id: str) -> IdentifiableEntity | None: #bea
        if not self.metadataQuery:
            return None

        for handler in self.metadataQuery:
            entity_df = handler.getById(id)

            if entity_df.empty:
                continue

            row = entity_df.loc[0]

            if not id.isdigit():
                person_uri = id
                result = Person(person_uri, row["author_name"]) #si chiama cosi quella row?
                return result

            # Assicurati che authors sia una lista
            authors = self.getAuthorsOfCulturalHeritageObject(id)

            base_url = "http://github.com/HelloKittyDataClan/DSexam/" 

            # Creazione dell'oggetto basato sul tipo
            if row["type"] == base_url + "NauticalChart":
                new_object = NauticalChart(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == base_url + "ManuscriptPlate":
                new_object = ManuscriptPlate(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == base_url + "ManuscriptVolume":
                new_object = ManuscriptVolume(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == base_url + "PrintedVolume":
                new_object = PrintedVolume(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == base_url + "PrintedMaterial":
                new_object = PrintedMaterial(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == "https://dbpedia.org/property/Herbarium":
                new_object = Herbarium(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == base_url + "Specimen":
                new_object = Specimen(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == "https://dbpedia.org/property/Painting":
                new_object = Painting(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == "https://dbpedia.org/property/Model":
                new_object = Model(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            elif row["type"] == "https://dbpedia.org/property/Map":
                new_object = Map(
                    id, row["title"], row["owner"], row["place"], authors, row["date"]
                )
            else:
                continue

            return new_object
        return None

        
    def getAllPeople(self):                                            #chiara
        people = []
        for handler in self.metadataQuery:
            people_data = handler.getAllPeople()
            for _, person_data in people_data.iterrows():
                person = Person(id=person_data['id_auth'], name=person_data['name_auth'])
                people.append(person)
        return people
        

    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:    #bea
        cultural_objects = {}

        for metadata in self.metadataQuery:
            # Ottieni il dataframe degli oggetti culturali
            df_objects = metadata.getAllCulturalHeritageObjects()
            for _, row in df_objects.iterrows():
                # Estrai le informazioni principali dell'oggetto culturale
                obj_id = str(row.id)
                title = row.title.strip()
                date = row.date if not pd.isna(row.date) else None 
                owner = row.owner
                place = row.place

                # Recupera gli autori per l'oggetto
                authors = []
                df_authors = metadata.getAuthorsOfCulturalHeritageObject(obj_id)
                for _, author_row in df_authors.iterrows():
                    author_id = author_row.authorID
                    author_name = author_row.authorName.strip()
                    author = Person(id=author_id, name=author_name)
                    authors.append(author)

                # Recupera la classe del sotto-tipo dell'oggetto dal tipo
                object_type = row.type.split("/")[-1]  # Estrai l'ultima parte dell'URI, es. "Map"
                
                # Ottieni la classe corrispondente dinamicamente
                obj_class = globals().get(object_type)  

                # Instanzia l'oggetto culturale
                obj_instance = obj_class(
                    id=obj_id,
                    title=title,
                    date=date,
                    owner=owner,
                    place=place,
                    authors=authors,
                )

                # Aggiungi l'oggetto alla lista, evitando duplicati
                cultural_objects[obj_id] = obj_instance

        return list(cultural_objects.values()) 

            
    
    def getAuthorsOfCulturalHeritageObject(self, id)->list[Person]:  #chiara          
        result = []
        dataf_list = []
        
        for handler in self.metadataQuery:
            dataf_list.append(handler.getAuthorsOfCulturalHeritageObject(id)) 
        dataf_union = pd.concat(dataf_list, ignore_index=True).fillna("")

        for idx, row in dataf_union.iterrows():
            author = row['authorName']
            if author != "":             
                object = Person(id=row["authorID"],name = row['authorName'])
                result.append(object)   
        return result
    

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> List[CulturalHeritageObject]:      #bea
        if not self.metadataQuery:
            raise ValueError("No metadata query handlers set.")
    
        object_list = []
    
        for handler in self.metadataQuery:
            objects_df = handler.getCulturalHeritageObjectsAuthoredBy(personId)
        
            for _, row in objects_df.iterrows():
                id = str(row['id'])  # Ensure ID is a string
                title = row['title']
                date = row.get('date')
                if date is not None and not isinstance(date, str):
                    date = str(date)  # Convert date to string if it's not None and not already a string
                owner = row['owner']
                place = row['place']
                author_name = row['authorName']
                author_id = str(row['authorID'])  # Ensure author ID is a string
                author = Person(id=author_id, name=author_name)

                obj_type = row['type'].split('/')[-1]
                cultural_obj = None
            
        
                if obj_type == 'NauticalChart':
                    cultural_obj = NauticalChart(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'ManuscriptPlate':
                    cultural_obj = ManuscriptPlate(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'ManuscriptVolume':
                    cultural_obj = ManuscriptVolume(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'PrintedVolume':
                    cultural_obj = PrintedVolume(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'PrintedMaterial':
                    cultural_obj = PrintedMaterial(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'Herbarium':
                    cultural_obj = Herbarium(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'Specimen':
                    cultural_obj = Specimen(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'Painting':
                    cultural_obj = Painting(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'Model':
                    cultural_obj = Model(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                elif obj_type == 'Map':
                    cultural_obj = Map(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                else:
                    cultural_obj = CulturalHeritageObject(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
            
                object_list.append(cultural_obj)

        return object_list

#ELENA
# mash up = when you fullifll you data model with the data from your queries 
    def getAllActivities(self) -> List[Activity]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        if not df_list:
            return []

        # Combine all DataFrames into one, drop duplicates, and fill missing values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        for _, row in df_union.iterrows():
            match_type = re.search(r'^[^-]*', row["internalId"])
            if match_type:
                activity_type = match_type.group(0)
                obj_refers_to = self.getEntityById(row["objectId"])

                if activity_type in dict_of_classes:
                    # Create the appropriate activity object
                    cls = dict_of_classes[activity_type]
                    if activity_type == 'acquisition':
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date'],
                            technique=row['technique']
                        )
                    else:
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date']
                        )
                    result.append(activity)

        return result

        
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> List[Activity]:
        result = []  # Initialize an empty list to store the results
        handler_list = self.processQuery  # List of query handlers
        df_list = []  # Temporary list to store DataFrames from each handler

        # Loop through each handler and gather activities based on responsible institution
        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsibleInstitution(partialName))

        # Return an empty list if no data is found
        if not df_list:
            return []

        # Combine all DataFrames into one, remove duplicates, and fill missing values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        # Dictionary mapping activity types to their corresponding classes
        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        # Filter the DataFrame to include only institutions matching the partialName
        df_union = df_union[df_union['responsible institute'].str.contains(partialName, case=False, na=False)]

        # Iterate over the filtered DataFrame and create corresponding activity objects
        for _, row in df_union.iterrows():
            # Extract activity type from internalId
            match_type = re.search(r'^[^-]*', row["internalId"])
            if match_type:
                activity_type = match_type.group(0)  # Get the activity type (e.g., acquisition, processing)
                obj_refers_to = self.getEntityById(row["objectId"])  # Retrieve the associated object by ID

                # Check if the activity type is valid and corresponds to a class
                if activity_type in dict_of_classes:
                    cls = dict_of_classes[activity_type]  # Get the corresponding class

                    # Instantiate the appropriate activity object
                    if activity_type == 'acquisition':
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date'],
                            technique=row['technique']  # Technique is specific to 'acquisition'
                        )
                    else:
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date']
                        )

                    # Append the created activity to the result list
                    result.append(activity)

        return result


    def getActivitiesByResponsiblePerson(self, partialName: str) -> List[Activity]:
        result = []  # Initialize an empty list to store the results
        handler_list = self.processQuery  # List of query handlers
        df_list = []  # Temporary list to store DataFrames from each handler

        # Loop through each handler and gather activities based on responsible person
        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsiblePerson(partialName))

        # Return an empty list if no data is found
        if not df_list:
            return []

        # Combine all DataFrames into one, remove duplicates, and fill missing values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        # Dictionary mapping activity types to their corresponding classes
        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        # Filter the DataFrame to include only persons matching the partialName
        df_union = df_union[df_union['responsible person'].str.contains(partialName, case=False, na=False)]

        # Iterate over the filtered DataFrame and create corresponding activity objects
        for _, row in df_union.iterrows():
            # Extract activity type from internalId
            match_type = re.search(r'^[^-]*', row["internalId"])
            if match_type:
                activity_type = match_type.group(0)  # Get the activity type (e.g., acquisition, processing)
                obj_refers_to = self.getEntityById(row["objectId"])  # Retrieve the associated object by ID

                # Check if the activity type is valid and corresponds to a class
                if activity_type in dict_of_classes:
                    cls = dict_of_classes[activity_type]  # Get the corresponding class

                    # Instantiate the appropriate activity object
                    if activity_type == 'acquisition':
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date'],
                            technique=row['technique']  # Technique is specific to 'acquisition'
                        )
                    else:
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date']
                        )

                    # Append the created activity to the result list
                    result.append(activity)

        return result


    def getActivitiesUsingTool(self, partialName: str) -> List[Activity]:
        result = []
        handler_list = self.processQuery
        df_list = []

        # Gather DataFrames from each handler
        for handler in handler_list:
            df_list.append(handler.getActivitiesUsingTool(partialName))

        # If no results, return an empty list
        if not df_list:
            return []

        # Combine all DataFrames into one, drop duplicates, and fill missing values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        for _, row in df_union.iterrows():
            match_type = re.search(r'^[^-]*', row["internalId"])
            if match_type:
                activity_type = match_type.group(0)
                obj_refers_to = self.getEntityById(row["objectId"])

                if activity_type in dict_of_classes:
                    cls = dict_of_classes[activity_type]
                    if activity_type == 'acquisition':
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date'],
                            technique=row['technique']
                        )
                    else:
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date']
                        )
                    result.append(activity)

        return result


    def getActivitiesStartedAfter(self, date: str) -> List[Activity]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        if not df_list:
            return []

        # Combine all DataFrames into one, drop duplicates, and fill missing values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        # Convert 'start date' to datetime, coercing errors to NaT (not a time)
        df_union['start date'] = pd.to_datetime(df_union['start date'], errors='coerce')

        # Filter out rows where 'start date' is missing or invalid
        df_filtered = df_union[df_union["start date"].notna()]
        df_filtered = df_filtered[df_filtered["start date"] >= date]

        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        for _, row in df_filtered.iterrows():
            match_type = re.search(r'^[^-]*', row["internalId"])
            if match_type:
                activity_type = match_type.group(0)
                obj_refers_to = self.getEntityById(row["objectId"])

                if activity_type in dict_of_classes:
                    cls = dict_of_classes[activity_type]
                    if activity_type == 'acquisition':
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date'],
                            technique=row['technique']
                        )
                    else:
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date']
                        )
                    result.append(activity)

        return result


    def getActivitiesEndedBefore(self, date: str) -> List[Activity]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        if not df_list:
            return []

        # Combine all DataFrames into one, drop duplicates, and fill missing values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        # Convert 'end date' to datetime, coercing errors to NaT (not a time)
        df_union['end date'] = pd.to_datetime(df_union['end date'], errors='coerce')

        # Filter out rows where 'end date' is missing or invalid
        df_filtered = df_union[df_union["end date"].notna()]
        df_filtered = df_filtered[df_filtered["end date"] < date]

        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        for _, row in df_filtered.iterrows():
            match_type = re.search(r'^[^-]*', row["internalId"])
            if match_type:
                activity_type = match_type.group(0)
                obj_refers_to = self.getEntityById(row["objectId"])

                if activity_type in dict_of_classes:
                    cls = dict_of_classes[activity_type]
                    if activity_type == 'acquisition':
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date'],
                            technique=row['technique']
                        )
                    else:
                        activity = cls(
                            object=obj_refers_to,
                            institute=row['responsible institute'],
                            person=row['responsible person'],
                            tool=row['tool'],
                            start=row['start date'],
                            end=row['end date']
                        )
                    result.append(activity)

        return result


    def getAcquisitionsByTechnique(self, partialName: str) -> list[Acquisition]:
        df = pd.DataFrame()
        handler_list = self.processQuery

        # Combine dataframes from all processes
        for handler in handler_list:
            df_process = handler.getAcquisitionsByTechnique(partialName)
            df = pd.concat([df, df_process], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)

        if df.empty:
            return []

        # Map acquisition types to corresponding classes
        activity_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        activities = []
        for _, row in df.iterrows():
            # Extract data for activity creation
            refers_to = self.getEntityById(str(row["objectId"]))
            institute = row["responsible institute"]
            person = row["responsible person"]
            start_date = row["start date"]
            end_date = row["end date"]
            tool = row["tool"]
            technique = row["technique"]
            
            # Determine the activity type
            activity_type = re.match(r'^[^-]*', row["internalId"]).group(0)
            
            # Create the activity object
            activity_class = activity_classes.get(activity_type)
            if activity_class:
                activity = activity_class(refers_to, institute, technique, person, start_date, end_date, tool)
                activities.append(activity)

        return activities


class AdvancedMashup(BasicMashup):
    def __init__(self):
        super().__init__()

    def getObjectsHandledByResponsiblePerson(self, partName: str) -> list[CulturalHeritageObject]: #chiara 

        obj_id = set()  

        for activity in self.getActivitiesByResponsiblePerson(partName):  
            obj_id.add(activity.refersTo().id)  

        cultural_objects = self.getAllCulturalHeritageObjects() 
        obj_list = []

        for obj in cultural_objects:
            if obj.id in obj_id:
                obj_list.append(obj) 
        return obj_list  
    

    def getObjectsHandledByResponsibleInstitution(self, partName: str) -> list[CulturalHeritageObject]:     #bea
        obj_id = set()  

        for activity in self.getActivitiesByResponsibleInstitution(partName):  
            obj_id.add(activity.refersTo().id)  

        cultural_objects = self.getAllCulturalHeritageObjects()  
        obj_list = []

        for obj in cultural_objects:
            if obj.id in obj_id:
                obj_list.append(obj) 

        return obj_list  
        
    
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]: #cata
    # Get the cultural heritage objects authored by the given person
        list_of_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        
        # Get all activities
        all_activities = self.getAllActivities()
        
        # Create a set of object IDs for faster lookup
        object_ids = {obj.id for obj in list_of_objects}
        
        # Filter activities where the referred object is in the object_ids set
        activities = [activity for activity in all_activities if activity.refersTo().id in object_ids]
        
        return activities
    

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]: #ele
        
        activities_after = self.getActivitiesStartedAfter(start)
        filtered_activities_after = [activity for activity in activities_after if isinstance(activity, Acquisition)]

        ids_of_filtered_objects = set()
        for act in filtered_activities_after:
            date = datetime.strptime(act.end, '%Y-%m-%d')
            if date <= datetime.strptime(end, '%Y-%m-%d'):
                ids_of_filtered_objects.add(act.refersTo().id)
        
        result_list = []
        all_authors = []
        for id in ids_of_filtered_objects:
            authors = self.getAuthorsOfCulturalHeritageObject(id)
            if authors:
                all_authors = all_authors + authors

        unique_ids = set()
        # Iterate over the list in reverse order
        for i in range(len(all_authors) - 1, -1, -1):
            author = all_authors[i]
            if author.id in unique_ids:
                del all_authors[i]  # Remove duplicate author
            else:
                unique_ids.add(author.id)
                result_list.append(author)
        
        return result_list





#------------------TEST-----------------------------
''' 
from tabulate import tabulate  # Libreria per il formato tabellare

# Step 1: Configura l'ambiente
rel_path = "relational.db"

# Step 2: Carica i dati di processo nel database relazionale
process = ProcessDataUploadHandler()
process.setDbPathOrUrl(rel_path)
process.pushDataToDb("data/process.json")

# Step 3: Carica i dati RDF nel database Blazegraph
pippo = MetadataUploadHandler()
output = pippo.setDbPathOrUrl("http://192.168.1.187:9999/blazegraph/")
output = pippo.pushDataToDb("data/meta.csv")
print("Metadata Upload Output:", output)

# Step 4: Configura il gestore delle query per Blazegraph
topolino = MetadataQueryHandler()
topolino.setDbPathOrUrl("http://192.168.1.187:9999/blazegraph/")

# Step 5: Configura il gestore dei processi
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

# Step 6: Configura e utilizza il mashup per ottenere i risultati combinati
masha = BasicMashup()
masha.metadataQuery = [topolino]

mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(topolino)
''' 



''' 

mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(topolino)

result = mashup.getObjectsHandledByResponsiblePerson("Grace Hopper")
pp(result)
'''


'''
#TEST FINALI DI VALENTINO

rel_path = "relational.db"
process = ProcessDataUploadHandler()
process.setDbPathOrUrl(rel_path)
process.pushDataToDb("data/process.json")
# Please remember that one could, in principle, push one or more files calling the method one or more times (even calling the method twice specifying the same file!)

metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl("http://192.168.1.75:9999/blazegraph/")
metadata.pushDataToDb("data/meta.csv")
# Please remember that one could, in principle, push one or more files calling the method one or more times (even calling the method twice specifying the same file!)

# In the next passage, create the query handlers for both the databases, using the related classes
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl("http://192.168.1.75:9999/blazegraph/")

masha = BasicMashup()
masha.metadataQuery = [metadata_qh]

result_1 = masha.getAllCulturalHeritageObjects

# Finally, create a advanced mashup object for asking about dataclear
mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(metadata_qh)

result_q6 = mashup.getActivitiesOnObjectsAuthoredBy("ULAN:500114874")

result_q7 = mashup.getObjectsHandledByResponsiblePerson("Alice Liddell")

'''
# Calling the method 1
'''all_activities = mashup.getAllActivities()
for i in all_activities:
    print(i.institute)'''

'''partial_name_institution = "Philology"
activities_by_institution = mashup.getActivitiesByResponsibleInstitution(partial_name_institution)
print(f"Activities by institution '{partial_name_institution}':")
for activity in activities_by_institution:
    print(activity)'''

'''partial_name_person = "Leonardo"
activities_by_person = mashup.getActivitiesByResponsiblePerson(partial_name_person)
print(f"Activities by person '{partial_name_person}':")
for person in activities_by_person:
    print(person)'''


'''partial_name_tool = "artec"
activities_by_tool = mashup.getActivitiesUsingTool(partial_name_tool)
print(f"Activities by tool '{partial_name_tool}':")
for activity in activities_by_tool:
    print(activity)'''

'''partial_name_start = "2023-07-14"
activities_by_start = mashup.getActivitiesStartedAfter(partial_name_start)
print(f"Activities by start date '{partial_name_start}':")
for activity in activities_by_start:
    print(activity)'''

'''# Call the method 6
partial_name_end = "2023-04-10"
activities_by_end = mashup.getActivitiesEndedBefore(partial_name_end)
print(f"Activities by end date '{partial_name_end}':")
for activity in activities_by_end:
    print(activity)

# Call the method 6
partial_name_technique = "Structured-light 3D scanner"
activities_by_technique = mashup.getAcquisitionsByTechnique(partial_name_technique)
print(f"Activities by technique '{partial_name_technique}':")
for activity in activities_by_technique:
    print(activity)'''