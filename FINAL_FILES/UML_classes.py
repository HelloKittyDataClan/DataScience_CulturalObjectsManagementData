class IdentifiableEntity(object): #identifichiamo l'ID
    def __init__(self, id: str):
        if not isinstance(id, str): #se l'ID non è una stringa
            raise ValueError("ID must be a string for the IdentifiableEntity")
        self.id = id #deve essere necessariamente una stringa cosi che l'ID sia valido sempre

    def get_id(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, id: str, name: str): # Modificato per includere l'ID
        super().__init__(id)
        if not isinstance(name, str):
            raise ValueError("Name must be a string for the Person")
        self.name = name
    
    def getName(self):
        return self.name

#___________________________CSV_________________________

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, owner: str, place: str, authors: list[Person], date:str = None):
        super().__init__(id)
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.authors = authors or []

    def getTitle(self) ->str:
        return self.title

    def getDate(self):
        if self.date == "":
            return None
        else:
            return self.date
        
    def getOwner(self) -> str:
        return self.owner

    def getPlace(self) -> str:
        return self.place

    def getAuthors(self) -> list[Person]:
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
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, refers_to:CulturalHeritageObject):
        self.institute = institute
        self.person = person
        self.tool = set()
        if isinstance(tool, str):
            self.tool = set(tool.split(", "))
        elif isinstance(tool, set):
            self.tool = tool         
        self.start = start
        self.end = end
        self.refers_to = refers_to
        
    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self):
        if self.person == "":
            return None
        else:
            return self.person
 
    def getTools(self):
        return {str(Tool) for Tool in self.tool}
    
    def getStartDate(self):
        if self.start == "":
            return None
        else:
            return self.start
    
    def getEndDate(self):
        if self.end == "":
            return None
        else:
            return self.end
    
    def refersTo(self) -> CulturalHeritageObject:   #---->>>non si riferisce a nessun oggetto e non ti ritorna nulla, TI DEVE RITORNARE CULTURAL OBJECT!!
       return self.refers_to
 
#Subclass of Activity just with technique parameter
 
class Acquisition(Activity):
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, technique: str, refers_to:CulturalHeritageObject):
        self.technique = technique
        super().__init__(institute, person, tool, start, end, refers_to) #for aquisition aggiungiamo il technique quindi la prima volta che lo definiamo è qui, dobbiamo ridichiarare perchè
        if not isinstance(technique, str):
            raise ValueError("Acquisition.technique must be a string")
        
    def getTechnique(self) -> str:
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