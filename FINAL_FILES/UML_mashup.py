import pandas as pd
from pandas import concat
from pprint import pprint
from UML_handler import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from UML_classes import IdentifiableEntity, Person, CulturalHeritageObject, Activity, Acquisition, Processing, Modelling, Optimising, Exporting, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map
from typing import Optional, List, Any, Dict


#____________________BasicMashup_____________________

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
    
    def getEntityById(self, id: str) -> Optional[IdentifiableEntity]:      #ritorna un oggetto della classe IdentifiableEntity identificando l'entità corrispondente all'identificatore dato nelle basi dati accessibili tramite i gestori di query; se non viene trovata nessuna entità con l'identificatore dato, ritorna None, assicurando che l'oggetto restituito appartenga alla classe appropriata.        
        for handler in self.metadataQuery:
            result = handler.getById(id)

            if not result.empty:
                entity_type = result.iloc[0].get('type')
                entity_id = result.iloc[0].get('id')
                entity_title = result.iloc[0].get('title')
                entity_owner = result.iloc[0].get('owner')
                entity_place = result.iloc[0].get('place')
                entity_date = result.iloc[0].get('date', None)
                entity_authors = result.iloc[0].get('authors', [])


                if entity_type == 'Person':
                    entity = Person(id=entity_id, name=entity_title)
                elif entity_type == 'NauticalChart':
                    entity = NauticalChart(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'ManuscriptPlate':
                    entity = ManuscriptPlate(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'ManuscriptVolume':
                    entity = ManuscriptVolume(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'PrintedVolume':
                    entity = PrintedVolume(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'PrintedMaterial':
                    entity = PrintedMaterial(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Herbarium':
                    entity = Herbarium(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Specimen':
                    entity = Specimen(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Painting':
                    entity = Painting(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Model':
                    entity = Model(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Map':
                    entity = Map(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                else:
                    entity = CulturalHeritageObject(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
            

                return entity

        
        return None


    

    def getAllPeople(self):                                            #restituisce la lista delle persone 
        # Ottieni tutte le persone usando MetadataQueryHandler
        people = []
        for handler in self.metadataQuery:
            people_data = handler.getAllPeople()
            for _, person_data in people_data.iterrows():
                person = Person(id=person_data['author_id'], name=person_data['author_name'])
                people.append(person)
        return people
    

    

    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]:     #restituisce una lista di oggetti della classe CulturalHeritageObject che comprende tutte le entità incluse nel database accessibili tramite i gestori di query, garantendo che gli oggetti nella lista siano della classe appropriata,        
        all_objects = []

        for handler in self.metadataQuery:
            results = handler.getAllCulturalHeritageObjects()

            for _, row in results.iterrows():
                obj_type = row['type']

                
                object_constructors = {
                    'Map': Map,
                    'Painting': Painting,
                    'Model': Model,
                    'Herbarium': Herbarium,
                    'Specimen': Specimen,
                    'ManuscriptPlate': ManuscriptPlate,
                    'ManuscriptVolume': ManuscriptVolume,
                    'PrintedVolume': PrintedVolume,
                    'PrintedMaterial': PrintedMaterial,
                }

                
                obj_constructor = object_constructors.get(obj_type, CulturalHeritageObject)

                
                obj = obj_constructor(
                    id=row['id'],
                    title=row['title'],
                    owner=row['owner'],
                    place=row['place'],
                    date=row['date'],
                    authors=row.get('authors', [])
                )

                all_objects.append(obj)
        
        return all_objects

        
    
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

    def getCulturalHeritageObjectsAuthoredBy(self, person_id: str) -> List[CulturalHeritageObject]:     #leggera modifica 
        if not self.metadataQuery:
            raise ValueError("No metadata query handlers set.")
    
        object_list = []
    
        for handler in self.metadataQuery:
            objects_df = handler.getCulturalHeritageObjectsAuthoredBy(person_id)
        
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
    def getAllActivities(self) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

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
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
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
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
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
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
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
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesStartedAfter(self, date: str) -> List[Any]:   #cata
        #should retrieves all activities that started after a specified date
        result = [] #empty list to store the filtered activities
        handler_list = self.processQuery #Get the list of process query handlers
        df_list = [] #to append the retrieved activities

        for handler in handler_list: #loop for each handler
            df_list.append(handler.getAllActivities())

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("") #concatenate all df in df_list into a single df_union. remove duplicates and fill any missing values with an empty str

        df_filtered = df_union[df_union["start date"] > date]

        for _, row in df_filtered.iterrows():
            activity_type, id = row["internalId"].split("-")  #create an instance of the corresponding activity
            obj_refers_to = self.getEntityById(row["objectId"]) #get the cultural heritage object referred to by the activity using its objectId

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
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
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refers_to=obj_refers_to )
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
                refers_to=obj_refers_to )
                result.append(object)

        return result



#___________________________ADVANCE MASHUP__________________________

class AdvancedMashup(BasicMashup):
    def __init__(self):
        self.activities = []  # Lista delle attività

    def getActivitiesOnObjectsAuthoredBy(self, personId:str):   #cata     #ID of the person whose authored objects we are interested in       
        cultural_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)  #calls a method that retrieves all cultural heritage objects authored by the person with the given id
        ids = []
        #Iteration over each object in cultural_objects
        for object in cultural_objects:
            ids.append(object.id)   #This creates a list of ids from the retrieved cultural heritage objects
        activities = self.getAllActivities() #retrieves all activities
        activities_list = []
        for activity in activities:
            if (activity.refersTo()).id in ids:
                activities_list.append(activity)  #activities, keeping only those that refer to an object whose id

        return activities_list  #list of activities


    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
        matched_objects = set()

        # Itera su tutte le attività
        for activity in self.activities:
            # Controlla se l'attività è gestita dall'istituzione responsabile specificata (anche parzialmente)
            if partialName.lower() in activity.getResponsibleInstitute().lower():
                # Recupera l'oggetto culturale a cui si riferisce l'attività
                cultural_heritage_object = activity.refersTo()
                # Aggiungi l'oggetto culturale all'insieme (l'insieme gestisce automaticamente l'unicità basata sull'ID)
                matched_objects.add(cultural_heritage_object)

        return list(matched_objects)



    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start:str, end:str):   #elena                  
        acquisition_start = [(i.refersTo()).id for i in self.getActivitiesStartedAfter(start) if type(i) is Acquisition]
        acquisition_end = [(i.refersTo()).id for i in self.getActivitiesEndedBefore(end) if type(i) is Acquisition]
        acquisition_list = [obj for obj in acquisition_start if obj in acquisition_end]
        authors_of_obj = set()
        for i in acquisition_list:
            authors = self.getAuthorsOfCulturalHeritageObject(str(i))
            for auth in authors:
                if auth is not None:
                    authors_of_obj.add((auth.id,auth.name))
        authors = [Person(id = auth[0],name=auth[1]) for auth in authors_of_obj]
        return authors