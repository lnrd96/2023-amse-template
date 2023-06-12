import pandas as pd
import numpy as np
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt

from database.model import Accident, Participants, Coordinate
from peewee import fn


class Explorer():
    """ Class to perform basic exploration of data properties, that
        is not considered data science yet. """

    def __init__(self):
        pass

    def plot_accident_location(self, target_file_name: str = None, n_accidents: int = 5000):
        """ Plots accidents on a map of germany.

        Args:
            target_file_name (str, optional): If not None provided file will be saved else showed. Defaults to None.
            n_accidents (int, optional): Number of accidents to plot. Random draws. Defaults to 5000.
        """

        # plot germany
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        germany = world[world['name'] == 'Germany']
        fig, ax = plt.subplots(1, 1)
        germany.plot(ax=ax, color='white', edgecolor='black')

        # plot coordinates

        for accident in Accident.select().order_by(fn.Random()).limit(n_accidents):
            longitude = accident.location.wsg_long
            latitude = accident.location.wsg_lat

            # TODO: distinguish between road types
            ax.plot(longitude, latitude, 'ro', markersize=1)  # 'ro' means red color, circle marker

        ax.set_title('Map of Accidents')
        ax.axis('off')
        plt.figure(figsize=(8, 6))  # adjust for the size of figure
        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)

    def plot_accidents_by_participants(self):
        """ Heatmap showing the frequency of pairs of participants
            involved in accidents.
        """
        def get_exclusive_count(participant):
            l_ps = participants_list.copy()
            l_ps.remove(participant)
            return (Accident
                    .select()
                    .join(Participants, on=(Accident.involved == Participants.id))
                    .where(participant == True & l_ps[0] == False &  # noqa: E712, W504
                           l_ps[1] == False & l_ps[2] == False &  # noqa: E712, W504
                           l_ps[3] == False & l_ps[4] == False)  # noqa: E712
                    .count()
                    )

        def get_joint_count(p1, p2):
            result = (Accident
                      .select()
                      .join(Participants, on=(Accident.involved == Participants.id))
                      .where((p1 == True) & (p2 == True))  # noqa: E712
                      .count()
                      )
            return result
        # number of accidents where predestrians are involved
        # c = Accident.select().where(Accident.involved.predestrian == True).count()
        participants_list = [Participants.predestrian, Participants.truck,
                             Participants.motorcycle, Participants.bike,
                             Participants.car, Participants.other]
        data = {
            'Predestrian': [get_joint_count(participants_list[0], participants_list[0]),
                            get_joint_count(participants_list[0], participants_list[1]),
                            get_joint_count(participants_list[0], participants_list[2]),
                            get_joint_count(participants_list[0], participants_list[3]),
                            get_joint_count(participants_list[0], participants_list[4]),
                            get_joint_count(participants_list[0], participants_list[5])],
            'Truck':       [get_joint_count(participants_list[1], participants_list[0]),
                            get_joint_count(participants_list[1], participants_list[1]),
                            get_joint_count(participants_list[1], participants_list[2]),
                            get_joint_count(participants_list[1], participants_list[3]),
                            get_joint_count(participants_list[1], participants_list[4]),
                            get_joint_count(participants_list[1], participants_list[5])],
            'Motorcycle':  [get_joint_count(participants_list[2], participants_list[0]),
                            get_joint_count(participants_list[2], participants_list[1]),
                            get_joint_count(participants_list[2], participants_list[2]),
                            get_joint_count(participants_list[2], participants_list[3]),
                            get_joint_count(participants_list[2], participants_list[4]),
                            get_joint_count(participants_list[2], participants_list[5])],
            'Bike':        [get_joint_count(participants_list[3], participants_list[0]),
                            get_joint_count(participants_list[3], participants_list[1]),
                            get_joint_count(participants_list[3], participants_list[2]),
                            get_joint_count(participants_list[3], participants_list[3]),
                            get_joint_count(participants_list[3], participants_list[4]),
                            get_joint_count(participants_list[3], participants_list[5])],
            'Car':         [get_joint_count(participants_list[4], participants_list[0]),
                            get_joint_count(participants_list[4], participants_list[1]),
                            get_joint_count(participants_list[4], participants_list[2]),
                            get_joint_count(participants_list[4], participants_list[3]),
                            get_joint_count(participants_list[4], participants_list[4]),
                            get_joint_count(participants_list[4], participants_list[5])],
            'Other':       [get_joint_count(participants_list[5], participants_list[0]),
                            get_joint_count(participants_list[5], participants_list[1]),
                            get_joint_count(participants_list[5], participants_list[2]),
                            get_joint_count(participants_list[5], participants_list[3]),
                            get_joint_count(participants_list[5], participants_list[4]),
                            get_joint_count(participants_list[5], participants_list[5])],
        }
        df = pd.DataFrame(data)
        plt.figure(figsize=(8, 6))  # adjust for the size of figure
        sns.heatmap(df, annot=True, cmap='coolwarm', fmt='d', vmax=200000, vmin=700)
        labels = ['Predestrian', 'Truck', 'Motorcycle', 'Bike', 'Car', 'Other']
        plt.xticks(np.arange(6) + 0.5, labels)
        plt.yticks(np.arange(6) + 0.5, labels, rotation=0)
        plt.savefig('jp.png')

    def plot_accidents_by_street_type(self, target_file_name: str = None):
        """ Plotting histograms frequency for respective road types. """

        # get data
        all_types_parsed = Accident.select(Accident.road_type_parsed).distinct()
        all_types_parsed = [item.road_type_parsed for item in all_types_parsed]
        all_types_osm = Accident.select(Accident.road_type_osm).distinct()
        all_types_osm = [item.road_type_osm for item in all_types_osm]
        histo_dict_parsed = {}
        for road in all_types_parsed:
            histo_dict_parsed[road] = Accident.select().where(Accident.road_type_parsed == road).count()
        histo_dict_osm = {}
        for road in all_types_osm:
            histo_dict_osm[road] = Accident.select().where(Accident.road_type_osm == road).count()

        # make histograms
        df_parsed = pd.DataFrame(list(histo_dict_parsed.items()), columns=['Category', 'Frequency'])
        df_osm = pd.DataFrame(list(histo_dict_osm.items()), columns=['Category', 'Frequency'])

        fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(8, 10))

        sns.barplot(x='Category', y='Frequency', data=df_parsed, palette='viridis', ax=axes[0])
        axes[0].set_title('Road type parsed out of street name')
        sns.barplot(x='Category', y='Frequency', data=df_osm, palette='viridis', ax=axes[1])
        axes[1].set_title('Road type by Open Street Map (OSM)')
        plt.setp(axes[0].xaxis.get_majorticklabels(), rotation=12)
        plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=90)
        plt.tight_layout()

        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)

    def plot_accidents_by_weekday(self, target_file_name: str = None):
        """ Plotting histograms frequency for respective week days. """

        all_days = Accident.select(Accident.weekday).distinct()
        all_days = [item.weekday for item in all_days]
        histo = {}
        for day in all_days:
            histo[day] = Accident.select().where(Accident.weekday == day).count()
        df = pd.DataFrame(list(histo.items()), columns=['Category', 'Frequency'])
        plt.figure(figsize=(8, 6))
        sns.barplot(x='Category', y='Frequency', data=df, palette='deep')
        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)
