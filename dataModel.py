class IdentifiableEntity(object): #identifichiamo l'ID
    def __init__(self, id:str):
        if not isinstance(id, str): #se l'ID non è una stringa
            raise ValueError("ID must be a string for the IdentifiableEntity")
        self.id = id #deve essere necessariamente una stringa cosi che l'ID sia valido sempre

    def get_id(self):
        return self.id
    

class CulturalObject(IdentifiableEntity):
    def __init__(self, id:str, title:str, date:str, owner:str, place:str):  #vado a definire title, date, owner e place del mio csv
        self.title=title
        self.date=date
        self.owner=owner
        self.place=place
        super().__init__(id) #cosi facendo vado a richiamare l'ID della classe IdentifiableEntity

    def getTitle(self):
        return self.title
    
    def getDate(self):
        return self.date
    
    def getOwner(self):
        return self.owner
    
    def getPlace(self):
        return self.place

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