# coding: utf-8
from sqlalchemy import ForeignKey, Column
from sqlalchemy.types import BigInteger, Float, Integer, JSON, String
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geography  # noqa: F401
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship, backref
from .database import session

Base = declarative_base()
Base.query = session.query_property()

class Place(Base):   # assume all places are relations
    __tablename__ = 'place'

    osm_id = Column(BigInteger, nullable=False)
    display_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    type = Column(String, nullable=False)
    place_id = Column(BigInteger, primary_key=True)
    place_rank = Column(Integer, nullable=False)
    icon = Column(String)
    geom = Column(Geography('GEOMETRY'), index=True)
    south = Column(Float, nullable=False)
    west = Column(Float, nullable=False)
    north = Column(Float, nullable=False)
    east = Column(Float, nullable=False)
    extratags = Column(JSON)
    address = Column(JSON)
    namedetails = Column(JSON)
    item_count = Column(Integer)
    candidate_count = Column(Integer)

    items = relationship('Item',
                         secondary='place_item',
                         lazy='dynamic',
                         backref=backref('places', lazy='dynamic'))

class Item(Base):
    __tablename__ = 'item'

    item_id = Column(Integer, primary_key=True)
    location = Column(Geography('POINT'), index=True, nullable=False)
    enwiki = Column(String, nullable=False)
    entity = Column(JSON)
    categories = Column(postgresql.ARRAY(String))
    tags = Column(postgresql.ARRAY(String))

    places = relationship('Place',
                          secondary='place_item',
                          lazy='dynamic',
                          backref=backref('items', lazy='dynamic'))

class PlaceItem(Base):
    __tablename__ = 'place_item'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    osm_id = Column(BigInteger, ForeignKey('place.osm_id'), primary_key=True)

    item = relationship('Item')
    place = relationship('Place')
