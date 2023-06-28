from tqdm import tqdm
import pandas as pd
from decimal import Decimal
from database.model import Accident, Coordinate, Participants


""" Fixing that in the first database fill run, the cars column was forgotten. """


df = pd.read_csv('/Users/leonardfischer/Uni/sem_2/de/2023-amse-template/project/unfallorte_all.csv')
num_not_found = 0
# print(Participants.delete().execute())

for frame in tqdm(df.itertuples(index=False)):
    frame = frame._asdict()
    coordinate, _ = Coordinate.get_or_create(utm_zone='32N',
                                             utm_x=Decimal(frame['LINREFX'].replace(',', '.')),
                                             utm_y=Decimal(frame['LINREFY'].replace(',', '.')),
                                             wsg_long=Decimal(frame['XGCSWGS84'].replace(',', '.')),
                                             wsg_lat=Decimal(frame['YGCSWGS84'].replace(',', '.')))
    accident = Accident.get_or_none(Accident.road_state == frame['STRZUSTAND'],
                                    Accident.severeness == frame['UKATEGORIE'] - 1,
                                    Accident.year == int(frame['UJAHR']),
                                    Accident.hour == int(frame['USTUNDE']),
                                    Accident.month == int(frame['UMONAT']),
                                    Accident.weekday == int(frame['UWOCHENTAG']),
                                    Accident.location == coordinate)
    if accident is not None:
        # make new participant entry
        participants, _ = \
            Participants.get_or_create(car=bool(frame['IstPKW']),
                                       predestrian=bool(frame['IstFuss']),
                                       truck=bool(frame['IstGkfz']),
                                       motorcycle=bool(frame['IstKrad']),
                                       bike=bool(frame['IstRad']),
                                       other=bool(frame['IstSonstig']))
        # update
        participants.save()
        accident.involved = participants
        accident.save()
    else:
        num_not_found += 1
        print(f'Accident not found. ({num_not_found})')

# query = Participants.delete().where(Participants.car.is_null())
# deleted_count = query.execute()
# print(f'Deleted {deleted_count} deprecated "Participants" entries.')
print(f'Updated {len(df)-num_not_found} Accident entries.')
