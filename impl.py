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
        self.authors = list() 

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



class Acquisition(Activity):
    def __init__(self, object: CulturalHeritageObject, institute: str, technique: str, person: str, start: str, end: str, tool: str|list[str]):

        super().__init__(object, institute, person, start, end, tool)  
        
        self.technique = technique 

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

#_______________Handlers_____________________


class Handler(object):  # chiara
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl

class UploadHandler(Handler):   #bea
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass

#___________________________GRAPH DATABASE_________________________

class MetadataUploadHandler(UploadHandler):  # chiara
    def __init__(self):  
        super().__init__()

    def pushDataToDb(self, path) -> bool:
            my_graph = Graph()
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
            venus.drop_duplicates(subset=["Id"], keep="first", inplace=True, ignore_index=True)

            base_url = Namespace("http://github.com/HelloKittyDataClan/DSexam/")
            db = Namespace("https://dbpedia.org/property/")
            schema = Namespace("http://schema.org/")

            my_graph.bind("base_url", base_url)
            my_graph.bind("db", db)
            my_graph.bind("FOAF", FOAF)
            my_graph.bind("schema", schema)

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


            title = URIRef(schema + "title")
            date = URIRef(schema + "dateCreated")
            place = URIRef(schema + "itemLocation")
            id = URIRef(schema + "identifier")
            owner = URIRef(base_url + "owner")

            relAuthor = URIRef(schema + "author")
          
            name = URIRef(FOAF + "name") 


           
            for idx, row in venus.iterrows():
                loc_id = "culturalobject-" + str(row["Id"])
                subj = URIRef(base_url + loc_id)
        
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
                if row["Title"] != "":
                    title_value = row["Title"].strip()
                    my_graph.add((subj, title, Literal(str(title_value)))) 
                if row["Date"] != "":
                    my_graph.add((subj, date, Literal(str(row["Date"]))))
                if row["Owner"] != "":
                    my_graph.add((subj, owner, Literal(str(row["Owner"]))))
                if row["Place"] != "":
                    my_graph.add((subj, place, Literal(str(row["Place"]))))

                if row["Author"] != "":
                    author_list = row["Author"].split(";")  
                    
                    for author in author_list:
                        if "(" in author and ")" in author:  
                            split_index = author.index("(")
                            author_name = author[:split_index - 1].strip()  
                            author_id = author[split_index + 1:-1].strip()  
                            
                            related_person = URIRef(base_url + "Person/" + author_id)
                            
                           
                            my_graph.add((subj, relAuthor, related_person))  
                            my_graph.add((related_person, name, Literal(author_name))) 
                            my_graph.add((related_person, id, Literal(author_id))) 
                            

          
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl() + "sparql"
            store.open((endpoint, endpoint)) 

            for triple in my_graph.triples((None, None, None)):
                store.add(triple)

            store.close()

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
        
        
#_____________________RELATIONALDATA BASE____________________________


class ProcessDataUploadHandler(UploadHandler):  #catalina
    def __init__(self):
        super().__init__()


    def pushDataToDbActivities(self, file_path: str, field_name: str) -> pd.DataFrame:

        with open(file_path, 'r') as file:
            df_activity = json.load(file)
        
        table_data: List[Dict[str, Any]] = []
        
        for item in df_activity:
            if field_name in item:
                field_entry = item[field_name]
                field_entry['object id'] = item['object id']
                table_data.append(field_entry)

     
        df_activities = pd.DataFrame(table_data)
        
      
        if 'tool' in df_activities.columns:
            df_activities['tool'] = df_activities['tool'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
            
        return df_activities

 
    def addInternalIds(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
      
        internal_ids = [f"{field_name}-{idx}" for idx in range(len(df))]
     
        df.insert(0, "internalId", Series(internal_ids, dtype="string"))
        
        return df

 
    def extractAndRenameColumns(self, df: pd.DataFrame, include_technique: bool = False) -> pd.DataFrame:
        columns = ["internalId", "object id", "responsible institute", "responsible person", "tool", "start date", "end date"]
        if include_technique:
            columns.insert(4, "technique") 
        
        identifiers = df[columns]
        identifiers = identifiers.rename(columns={"object id": "objectId"})
        return identifiers
        
 
    def pushDataToDb(self, activities_file_path: str):
      
        acquisition_df = self.pushDataToDbActivities(activities_file_path, 'acquisition')
        processing_df = self.pushDataToDbActivities(activities_file_path, 'processing')
        modelling_df = self.pushDataToDbActivities(activities_file_path, 'modelling')
        optimising_df = self.pushDataToDbActivities(activities_file_path, 'optimising')
        exporting_df = self.pushDataToDbActivities(activities_file_path, 'exporting')

     
        acquisition_df = self.addInternalIds(acquisition_df, 'acquisition')
        processing_df = self.addInternalIds(processing_df, 'processing')
        modelling_df = self.addInternalIds(modelling_df, 'modelling')
        optimising_df = self.addInternalIds(optimising_df, 'optimising')
        exporting_df = self.addInternalIds(exporting_df, 'exporting')
        

        acquisition_final_db = self.extractAndRenameColumns(acquisition_df, include_technique=True)
        processing_final_db = self.extractAndRenameColumns(processing_df)
        modelling_final_db = self.extractAndRenameColumns(modelling_df)
        optimising_final_db = self.extractAndRenameColumns(optimising_df)
        exporting_final_db = self.extractAndRenameColumns(exporting_df)
        

        with connect("relational.db") as con:
            acquisition_final_db.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_final_db.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_final_db.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_final_db.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_final_db.to_sql("Exporting", con, if_exists="replace", index=False)
        
        return True

#_____________________QueryHandler____________________________

class QueryHandler(Handler):
    def __init__(self,):  
        super().__init__()
    

    def getById(self, id: str) -> pd.DataFrame:     #beatrice
        id = str(id)
        
        if self.getDbPathOrUrl().startswith("http"):
            db_address = self.getDbPathOrUrl()
        else:
            return pd.DataFrame() 
        

        endpoint = db_address + "sparql" if db_address.endswith("/") else db_address + "/sparql"


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
    
 #_____________________MetadataQueryHandler____________________________   

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

    
    def getAllCulturalHeritageObjects(self):        #beatrice
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
    
    
    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> pd.DataFrame:          #beatrice
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

 #_____________________ProcessDataQueryHandler____________________________   

class ProcessDataQueryHandler(QueryHandler): #elena
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

 #_____________________BasicMashup____________________________   

class BasicMashup(object):
    def __init__(self) -> None:
        self.metadataQuery = list()
        self.processQuery = list()
    
    def cleanMetadataHandlers(self) -> bool:    #chiara
        self.metadataQuery = []
        return True

    def cleanProcessHandlers(self) -> bool:      #catalina
        self.processQuery = [] 
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:     #beatrice
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler:ProcessDataQueryHandler) -> bool:   #elena
        if not isinstance(handler, ProcessDataQueryHandler):
            return False
        else:
            self.processQuery.append(handler)  
            return True
        
    def getEntityById(self, id: str) -> IdentifiableEntity | None: #beatrice
        if not self.metadataQuery:
            return None

        for handler in self.metadataQuery:
            entity_df = handler.getById(id)

            if entity_df.empty:
                continue

            row = entity_df.loc[0]

            if not id.isdigit():
                person_uri = id
                result = Person(person_uri, row["author_name"])
                return result

          
            authors = self.getAuthorsOfCulturalHeritageObject(id)

            base_url = "http://github.com/HelloKittyDataClan/DSexam/" 

           
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
        

    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:    #beatrice
        cultural_objects = {}

        for metadata in self.metadataQuery:
            
            df_objects = metadata.getAllCulturalHeritageObjects()
            for _, row in df_objects.iterrows():
                obj_id = str(row.id)
                title = row.title.strip()
                date = row.date if not pd.isna(row.date) else None 
                owner = row.owner
                place = row.place

             
                authors = []
                df_authors = metadata.getAuthorsOfCulturalHeritageObject(obj_id)
                for _, author_row in df_authors.iterrows():
                    author_id = author_row.authorID
                    author_name = author_row.authorName.strip()
                    author = Person(id=author_id, name=author_name)
                    authors.append(author)

              
                object_type = row.type.split("/")[-1]  
                
               
                obj_class = globals().get(object_type)  

               
                obj_instance = obj_class(
                    id=obj_id,
                    title=title,
                    date=date,
                    owner=owner,
                    place=place,
                    authors=authors,
                )

               
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
    

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> List[CulturalHeritageObject]:      #beatrice
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


    def getAllActivities(self) -> List[Activity]:   #elena
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        if not df_list:
            return []

       
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
        result = []  
        handler_list = self.processQuery 
        df_list = [] 

       
        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsibleInstitution(partialName))

      
        if not df_list:
            return []

       
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")


        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

       
        df_union = df_union[df_union['responsible institute'].str.contains(partialName, case=False, na=False)]

       
        for _, row in df_union.iterrows():
            # Extract activity type from internalId
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


    def getActivitiesByResponsiblePerson(self, partialName: str) -> List[Activity]:
        result = []  
        handler_list = self.processQuery  
        df_list = []  

        
        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsiblePerson(partialName))

       
        if not df_list:
            return []

       
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        
        dict_of_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

       
        df_union = df_union[df_union['responsible person'].str.contains(partialName, case=False, na=False)]

        
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


    def getActivitiesUsingTool(self, partialName: str) -> List[Activity]:
        result = []
        handler_list = self.processQuery
        df_list = []

       
        for handler in handler_list:
            df_list.append(handler.getActivitiesUsingTool(partialName))

      
        if not df_list:
            return []

      
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

       
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

       
        df_union['start date'] = pd.to_datetime(df_union['start date'], errors='coerce')

       
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

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        df_union['end date'] = pd.to_datetime(df_union['end date'], errors='coerce')

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

        
        for handler in handler_list:
            df_process = handler.getAcquisitionsByTechnique(partialName)
            df = pd.concat([df, df_process], ignore_index=True).drop_duplicates()
            df.fillna('', inplace=True)

        if df.empty:
            return []

       
        activity_classes = {
            'acquisition': Acquisition,
            'processing': Processing,
            'modelling': Modelling,
            'optimising': Optimising,
            'exporting': Exporting
        }

        activities = []
        for _, row in df.iterrows():
            
            refers_to = self.getEntityById(str(row["objectId"]))
            institute = row["responsible institute"]
            person = row["responsible person"]
            start_date = row["start date"]
            end_date = row["end date"]
            tool = row["tool"]
            technique = row["technique"]
            
          
            activity_type = re.match(r'^[^-]*', row["internalId"]).group(0)
            
           
            activity_class = activity_classes.get(activity_type)
            if activity_class:
                activity = activity_class(refers_to, institute, technique, person, start_date, end_date, tool)
                activities.append(activity)

        return activities

 #_____________________AdvancedMashup____________________________   

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
    

    def getObjectsHandledByResponsibleInstitution(self, partName: str) -> list[CulturalHeritageObject]:     #beatrice
        obj_id = set()  

        for activity in self.getActivitiesByResponsibleInstitution(partName):  
            obj_id.add(activity.refersTo().id)  

        cultural_objects = self.getAllCulturalHeritageObjects()  
        obj_list = []

        for obj in cultural_objects:
            if obj.id in obj_id:
                obj_list.append(obj) 

        return obj_list  
        
    
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]: #catalina

        list_of_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
   
        all_activities = self.getAllActivities()
        
        object_ids = {obj.id for obj in list_of_objects}
        
        activities = [activity for activity in all_activities if activity.refersTo().id in object_ids]
        
        return activities
    

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]: #elena
        
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
        
        for i in range(len(all_authors) - 1, -1, -1):
            author = all_authors[i]
            if author.id in unique_ids:
                del all_authors[i]  
            else:
                unique_ids.add(author.id)
                result_list.append(author)
        
        return result_list


