import os
from config import PROJECT_ROOT
from peewee import *  # noqa: F403

DB = SqliteDatabase(os.path.join(PROJECT_ROOT, 'database', 'data.sqlite'))


class Participants(Model):
    """ Type of participants involved into the accident. """
    car = BooleanField()
    predestrian = BooleanField()
    truck = BooleanField()  # Gkfz
    motorcycle = BooleanField()  # Krad
    bike = BooleanField()
    other = BooleanField()

    class Meta:
        database = DB


class Coordinate(Model):
    """ Two representations present.

        Universal Transverse Mercator (UTM) System.
        Decimal places are for submeter precision

        WGS84 World Geodetic System 1984)
    """
    utm_zone = CharField()
    utm_x = DecimalField(max_digits=7, decimal_places=1, auto_round=True)
    utm_y = DecimalField(max_digits=7, decimal_places=1, auto_round=True)
    wsg_long = DecimalField(max_digits=8, decimal_places=6, auto_round=True)  # x
    wsg_lat = DecimalField(max_digits=8, decimal_places=6, auto_round=True)  # y

    class Meta:
        database = DB


class Accident(Model):
    """ The accident itself referencing the other relations
        and holding some metadata specific to the accident.
    """
    road_state = IntegerField()  # 0:=dry, 1:=wet, 2:=frozen
    severeness = IntegerField()  # 0:=minor injuries, 1:= major injuries, 2:= deadly UKATEGORIE
    lighting_conditions = IntegerField()  # 0:=daylight, 1:=dusk, 2:=dark
    road_type_osm = CharField()
    road_type_parsed = CharField()
    involved = ForeignKeyField(Participants)
    location = ForeignKeyField(Coordinate)
    year = IntegerField()
    month = IntegerField()
    hour = IntegerField()
    weekday = IntegerField()

    class Meta:
        database = DB
