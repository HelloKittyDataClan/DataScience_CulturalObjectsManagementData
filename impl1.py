from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.namespace import FOAF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import csv
import json

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

    def get_id(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, id: str, name: str):
        self.name = name
        super().__init__(id)
    
    def getName(self):
        return self.name

#___________________________CSV_________________________

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, owner: str, place: str, hasAuthor: list[Person], date:str = None):
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.hasAuthor = hasAuthor 
        super().__init__(id)

    def getTitle(self) ->str:
        return self.title

     
    def getDate(self) -> Optional[str]:
        return self.date

        
    def getOwner(self) -> str:
        return self.owner

    def getPlace(self) -> str:
        return self.place

    def getAuthors(self) -> list[Person]:
        return self.hasAuthor
    

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
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str|None=None, start: str|None=None, end: str|None=None, tool: str|list[str]|None=None):
        self.refersTo_cho = refersTo_cho
        self.institute = institute
        self.person = person     
        self.start = start
        self.end = end

        self.tool = []

        if type(tool) == str:
            self.tool.append(tool)
        elif type(tool) == list:
            self.tool = tool
        
    def getResponsibleInstitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        if self.person:
            return self.person 
        return None
    
    def getTools(self): 
        return self.tool
    
    def getStartDate(self):
        if self.star:
            return self.start
        return None

    def getEndDate(self):
        if self.star:
            return self.end
        return None
        
    def refersTo(self):
        return self.refersTo_cho

#Subclass of Activity just with technique parameter

class Acquisition(Activity):
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, technique: str, person: str|None=None, start: str|None=None, end: str|None=None, tool: str|list[str]|None=None):

        super().__init__(refersTo_cho, institute, person, tool, start, end)

        self.technique = technique
    
    def getTechnique(self):
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass
 

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

    def pushDataToDb(self, path):
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
    
    
    def getCulturalHeritageObjectsAuthoredBy(self, personid: str) -> pd.DataFrame:          #bea
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
query_handler = ProcessDataQueryHandler()

# Set the database path or URL
query_handler.setDbPathOrUrl("relational.db")

# Call the method and store the result
getActivitiesByResponsiblePerson = query_handler.getActivitiesByResponsiblePerson("Grace")

pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
# Print the result
print("Result from getActivitiesByResponsibleInstitution:")
print(getActivitiesByResponsiblePerson)
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
        self.processQuery.clear()  #clear the process handlers list
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:     #bea
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler:'ProcessDataQueryHandler') -> bool:   #elena
        if not isinstance(handler, ProcessDataQueryHandler):
        # check if handler is an istance of processdataqueryhandler (prevention)
            return False
        else:
            self.processQuery.append(handler)  # Adds a process handler to the list
            return True
        
    def getEntityById(self, related_id: str):   #bea
        if not self.metadataQuery:
            return None

        for handler in self.metadataQuery:
            entity_df = handler.getById(related_id)

            if entity_df.empty:
                continue

            row = entity_df.loc[0]

            if not related_id.isdigit():
                person_uri = related_id
                result = Person(person_uri, row["author_name"])
                return result

            # Assicurati che authors sia una lista
            authors = self.getAuthorsOfCulturalHeritageObject(related_id)

            base_url = "http://github.com/HelloKittyDataClan/DSexam/" 

            # Creazione dell'oggetto basato sul tipo
            if row["type"] == base_url + "NauticalChart":
                new_object = NauticalChart(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == base_url + "ManuscriptPlate":
                new_object = ManuscriptPlate(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == base_url + "ManuscriptVolume":
                new_object = ManuscriptVolume(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == base_url + "PrintedVolume":
                new_object = PrintedVolume(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == base_url + "PrintedMaterial":
                new_object = PrintedMaterial(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == "https://dbpedia.org/property/Herbarium":
                new_object = Herbarium(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == base_url + "Specimen":
                new_object = Specimen(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == "https://dbpedia.org/property/Painting":
                new_object = Painting(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == "https://dbpedia.org/property/Model":
                new_object = Model(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
                )
            elif row["type"] == "https://dbpedia.org/property/Map":
                new_object = Map(
                    row["title"], row["owner"], row["place"], authors, row["date"], related_id
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
                    hasAuthor=authors,
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
    

    def getCulturalHeritageObjectsAuthoredBy(self, personid: str) -> List[CulturalHeritageObject]:      #bea
        if not self.metadataQuery:
            raise ValueError("No metadata query handlers set.")
    
        object_list = []
    
        for handler in self.metadataQuery:
            objects_df = handler.getCulturalHeritageObjectsAuthoredBy(personid)
        
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
                    cultural_obj = NauticalChart(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'ManuscriptPlate':
                    cultural_obj = ManuscriptPlate(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'ManuscriptVolume':
                    cultural_obj = ManuscriptVolume(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'PrintedVolume':
                    cultural_obj = PrintedVolume(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'PrintedMaterial':
                    cultural_obj = PrintedMaterial(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Herbarium':
                    cultural_obj = Herbarium(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Specimen':
                    cultural_obj = Specimen(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Painting':
                    cultural_obj = Painting(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Model':
                    cultural_obj = Model(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Map':
                    cultural_obj = Map(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                else:
                    cultural_obj = CulturalHeritageObject(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
            
                object_list.append(cultural_obj)

        return object_list
        

#ELENA
    def getAllActivities(self) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        if not df_list:
            return pd.DataFrame()

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])
            

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsibleInstitution(partialName))

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesByResponsiblePerson(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsiblePerson(partialName))

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesUsingTool(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesUsingTool(partialName))

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesStartedAfter(self, date: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        df_filtered = df_union[df_union["start date"] > date]

        for _, row in df_filtered.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesEndedBefore(self, date: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        df_filtered = df_union[df_union["end date"] < date]

        for _, row in df_filtered.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)


        return result

    def getAcquisitionsByTechnique(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        # Collect activities from handlers
        for handler in handler_list:
            activities = handler.getAllActivities()
            if activities is not None and not activities.empty:
                df_list.append(activities)
            else:
                print(f"No activities found in handler: {handler}")

        # Check if df_list is not empty
        if not df_list:
            print("No activities found in any handler.")
            return result

        # Concatenate DataFrames
        try:
            df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
        except ValueError as e:
            print(f"Error concatenating DataFrames: {e}")
            return result

        # Filter acquisitions by technique
        try:
            df_filtered = df_union[df_union["technique"].str.contains(partialName, case=False, na=False)] 
        except KeyError as e:
            print(f"Key error: {e}")
            return result

        # Process each filtered row
        for _, row in df_filtered.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result


mashup = BasicMashup()
process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("relational.db")

# Adding process query handler to mashup
mashup.addProcessHandler(process_query)

'''
# Calling the method 1
all_activities = mashup.getAllActivities()
for i in all_activities:
    print(i.institute)

# Call the method 2
partial_name_person = "Grace"
all_responsible_person = mashup.getActivitiesByResponsiblePerson(partial_name_person)
print(f"Activities associated with the responsible person '{partial_name_person}':")
for activity in all_responsible_person:
   print(activity.person)

# Call the method 3
partial_name_tool = "Blender"
activities_by_tool = mashup.getActivitiesUsingTool(partial_name_tool)
for activity in activities_by_tool:
    subclass_name = activity.__class__.__name__
    print(f"Activity: {subclass_name}, Tool: {partial_name_tool}")


# Call the method 4
partial_name_institution = "Philology"
activities_by_institution = mashup.getActivitiesByResponsibleInstitution(partial_name_institution)
activity_summary = {}
for activity in activities_by_institution:
    activity_type = activity.__class__.__name__
    activity_summary[activity_type] = activity_summary.get(activity_type, 0) + 1
print(f"Activities by institution '{partial_name_institution}':")
for activity_type, count in activity_summary.items():
    print(f"- {activity_type}: {count} occurrences")

# Call the method 5
partial_name_start = "2023-11-05"
activities_by_start = mashup.getActivitiesStartedAfter(partial_name_start)
activity_summary = {}
for activity in activities_by_start:
    activity_type = activity.__class__.__name__
    activity_summary[activity_type] = activity_summary.get(activity_type, 0) + 1
print(f"Activities started after '{partial_name_start}':")
for activity_type, count in activity_summary.items():
    print(f"- {activity_type}: {count} occurrences")

# Call the method 6
partial_name_end = "2023-02-10"
activities_by_end = mashup.getActivitiesEndedBefore(partial_name_end)
activity_summary = {}
for activity in activities_by_end:
    activity_type = activity.__class__.__name__
    activity_summary[activity_type] = activity_summary.get(activity_type, 0) + 1
print(f"Activities ended before '{partial_name_end}':")
for activity_type, count in activity_summary.items():
    print(f"- {activity_type}: {count} occurrences")

# Call the method 7
partial_name_technique = "Structured-light 3D scanner"
activities_by_technique = mashup.getAcquisitionsByTechnique(partial_name_technique)
activity_summary = {}
for activity in activities_by_technique:
    activity_type = activity.__class__.__name__
    activity_summary[activity_type] = activity_summary.get(activity_type, 0) + 1
print(f"Activities by technique '{partial_name_technique}':")
for activity_type, count in activity_summary.items():
    print(f"- {activity_type}: {count} occurrences")
'''

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
        

    
    def getActivitiesOnObjectsAuthoredBy(self, personId: str):
        cultural_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        id_list = []
        for cultural_object in cultural_objects:
            object_id = cultural_object.id
            id_list.append(object_id)

        activities = self.getAllActivities()
        result_list = []
        for activity in activities:

            referred_object = activity.refersTo()
            referred_object_id = referred_object.id
            if referred_object_id in id_list:
                result_list.append(activity)
        return result_list
    

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str):
        activities_started_after_start = self.getActivitiesStartedAfter(start)
        acquisition_start = []
        for activity in activities_started_after_start:
            if type(activity) is Acquisition:
                referred_object_id = (activity.refersTo()).id
                acquisition_start.append(referred_object_id)
        activities_ended_before_end = self.getActivitiesEndedBefore(end)
        acquisition_end = []
        for activity in activities_ended_before_end:
            if type(activity) is Acquisition:
                referred_object_id = (activity.refersTo()).id
                acquisition_end.append(referred_object_id)
        acquisition_list = []
        for obj_id in acquisition_start:
            if obj_id in acquisition_end:
                acquisition_list.append(obj_id)
        authors_of_obj = set()
        for obj_id in acquisition_list:
            authors = self.getAuthorsOfCulturalHeritageObject(str(obj_id))
            for author in authors:
                if author is not None:
                    authors_of_obj.add((author.id, author.name))
        authors_list = []
        for author_tuple in authors_of_obj:
            person = Person(id=author_tuple[0], name=author_tuple[1])
            authors_list.append(person)
        return authors_list
    
'''pu=ProcessDataUploadHandler()
mu = MetadataUploadHandler()
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
path_process = 
path_meta = 
pu.setDbPathOrUrl("activities.db")
pu.pushDataToDb(path_process)
mu.setDbPathOrUrl(grp_endpoint)
mu.pushDataToDb(path_meta)
pq = ProcessDataQueryHandler()
mq = MetadataQueryHandler()
pq.setDbPathOrUrl("activities.db")
mq.setDbPathOrUrl(grp_endpoint)
am = AdvancedMashup()
am.addProcessHandler(pq)
am.addMetadataHandler(mq)

for i in range(1,36): 
    obj = am.getEntityById(str(i))
    print(obj.getId(), obj.getTitle(), obj.getDate(), obj.getOwner(), obj.getPlace(), obj.getAuthors())
    '''
'''
#------------------TEST-----------------------------
from tabulate import tabulate  # Libreria per il formato tabellare

# Launch ProcessDataUploadHandler() Class
process = ProcessDataUploadHandler()
process.setDbPathOrUrl("." + sep + "relational.db")
process.pushDataToDb("data" + sep + "process.json")

# Launch MeatadataUploadHandler() Class
grp_endpoint = "http://192.168.1.15:9999/blazegraph/"
metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl(grp_endpoint)
metadata.pushDataToDb("data" + sep + "meta.csv")

# Launch QueryHandler() Class
queryHandler = QueryHandler()
queryHandler.setDbPathOrUrl("http://192.168.1.15:9999/blazegraph/")
queryHandler.getById("VIAF:265397758")

# Launch MetadataQueryHandler() Class
metaData_qh = MetadataQueryHandler()
metaData_qh.setDbPathOrUrl("http://192.168.1.15:9999/blazegraph/")
metaData_qh.getAllPeople()

# Launch ProcessDataQueryHandler() Class
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl("." + sep + "relational.db")
process_qh.getAllActivities()

# Launch BasicMashup() Class
mashup = BasicMashup()
mashup.addMetadataHandler(metaData_qh)
mashup.addProcessHandler(process_qh)
mashup.getCulturalHeritageObjectsAuthoredBy("ULAN:500114874")
mashup.getActivitiesByResponsiblePerson("Ada Lovelace")

# Launch AdvancedMashup() Class
adv_mashup = AdvancedMashup()
adv_mashup.addMetadataHandler(metaData_qh)
adv_mashup.addProcessHandler(process_qh)
adv_mashup.getActivitiesOnObjectsAuthoredBy("VIAF:78822798")
adv_mashup.getObjectsHandledByResponsibleInstitution("HSE")
adv_mashup.getAuthorsOfObjectsAcquiredInTimeFrame('2015-03-04', '2018-05-10')


#------------------TEST-----------------------------

from tabulate import tabulate  # Libreria per il formato tabellare

# Step 1: Configura l'ambiente
rel_path = "relational.db"

# Step 2: Carica i dati di processo nel database relazionale
process = ProcessDataUploadHandler()
process.setDbPathOrUrl(rel_path)
process.pushDataToDb("data/process.json")

# Step 3: Carica i dati RDF nel database Blazegraph
pippo = MetadataUploadHandler()
output = pippo.setDbPathOrUrl("http://192.168.1.65:9999/blazegraph/")
output = pippo.pushDataToDb("data/meta.csv")
print("Metadata Upload Output:", output)

# Step 4: Configura il gestore delle query per Blazegraph
topolino = MetadataQueryHandler()
topolino.setDbPathOrUrl("http://192.168.1.65:9999/blazegraph/")

# Step 5: Configura il gestore dei processi
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

# Step 6: Configura e utilizza il mashup per ottenere i risultati combinati
masha = BasicMashup()
masha.metadataQuery = [topolino]

mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(topolino)


mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(topolino)

result = mashup.getObjectsHandledByResponsiblePerson("Grace Hopper")
pp(result)


#TEST FINALI DI VALENTINO

rel_path = "relational.db"
process = ProcessDataUploadHandler()
process.setDbPathOrUrl(rel_path)
process.pushDataToDb("data/process.json")
# Please remember that one could, in principle, push one or more files calling the method one or more times (even calling the method twice specifying the same file!)

metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl("http://192.168.0.190:9999/blazegraph/")
metadata.pushDataToDb("data/meta.csv")
# Please remember that one could, in principle, push one or more files calling the method one or more times (even calling the method twice specifying the same file!)

# In the next passage, create the query handlers for both the databases, using the related classes
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl("http://192.168.0.190:9999/blazegraph/")

masha = BasicMashup()
masha.metadataQuery = [metadata_qh]

result_1 = masha.getAllCulturalHeritageObjects

# Finally, create a advanced mashup object for asking about dataclear
mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(metadata_qh)

'''
