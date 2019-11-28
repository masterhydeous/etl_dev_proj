from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, 
    Integer, 
    String, 
    ForeignKey,
)
Base = declarative_base()
class ClientLocation(Base):
    __tablename__ = 'clientloc'
    id = Column(Integer, primary_key=True)
    Latitude = Column(String(255),unique=False)
    Longitude = Column(String(255),unique=False)
    Rua = Column(String(255),unique=False)
    Numero = Column(String(255),unique=False)
    Bairro = Column(String(255),unique=False)
    Cidade = Column(String(255),unique=False)
    Cep = Column(String(255),unique=False)
    Estado = Column(String(255),unique=False)
    Pais = Column(String(255),unique=False)
