import json

#UTF-8

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
    def __init__(self, id:str, title:str, owner:str, place:str, date:str= None): #vado a definire title, date, owner e place del mio csv
        super().__init__(id)  #cosi facendo vado a richiamare l'ID della classe IdentifiableEntity
        if not isinstance(title, str):
            raise ValueError("Title must be a string for the CulturalObject")
        if not isinstance(owner, str):
            raise ValueError("Owner must be a string for the CulturalObject")
        if not isinstance(place, str):
            raise ValueError("Place must be a string for the CulturalObject")
        if date is not None and not isinstance(date, str):
            raise ValueError("Date must be a string or None for the CulturalObject")
        self.title=title
        self.date=date
        self.owner=owner
        self.place=place
        self.authors=[]
        

    def getTitle(self):
        return self.title

    def getDate(self):
        return self.date

    def getOwner(self):
        return self.owner

    def getPlace(self):
        return self.place

    def getAuthors(self):
        return self.authors

    def addAuthor(self, author):
        if isinstance(author, Person):
            self.authors.append(author)
        else:
            raise ValueError("Author must be an instance of Person")

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


#dopo aver avuto la parte .json, connettere has Author to Person e Activity refers to la classe Culture Object

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
class Activity(CulturalObject):
    def _init_(self, id:str, institute: str, person: str= None, tool: str|set[str]|None = None, start: str = None, end: str = None):
        super().__init__(id, "", "")  # Initialize ID, title, owner, place (replace with appropriate values)
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
        self.tool = {}
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
    
    def refersTo(self, CulturalObject):
        if isinstance(CulturalObject, CulturalObject):
            self.title.append(CulturalObject)
        else:
            raise ValueError("Invalid object type provided")

#Subclass of Activity just with technique parameter

class Acquisition(Activity):
    def _init_(self, id: str, technique: str):
        super().__init__(id) 
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
