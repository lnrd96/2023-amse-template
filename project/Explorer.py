import pandas as pd
import numpy as np
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
import peewee
import datetime
import calendar

from database.model import Accident, Participants, Coordinate
from calendar import monthrange
from peewee import fn


class Explorer():
    """ Class to perform basic exploration of data properties, that
        is not considered data science yet. """

    def __init__(self):
        self.participants_list = [Participants.predestrian, Participants.truck,
                                  Participants.motorcycle, Participants.bike,
                                  Participants.car, Participants.other]

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

    def _plot_query_on_map(self, query: peewee.ModelSelect):
        """ Plots accidents present in query on a map of germany.
        """

        # plot germany
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        germany = world[world['name'] == 'Germany']
        fig, ax = plt.subplots(1, 1)
        germany.plot(ax=ax, color='white', edgecolor='black')

        # plot coordinates

        for accident in query:
            longitude = accident.location.wsg_long
            latitude = accident.location.wsg_lat

            # TODO: distinguish between road types
            ax.plot(longitude, latitude, 'ro', markersize=1)  # 'ro' means red color, circle marker

        ax.set_title('Map of Accidents')
        ax.axis('off')
        plt.figure(figsize=(8, 6))  # adjust for the size of figure
        plt.show()

    def plot_percentage_of_injury_types(self):
        """ Pieplot of accident severeness.
        """
        all_accidents = Accident.select().count()
        deadly_accidents = Accident.select().where(Accident.severeness == 0).count()
        major = Accident.select().where(Accident.severeness == 1).count()
        minor = Accident.select().where(Accident.severeness == 2).count()
        assert deadly_accidents + minor + major == all_accidents
        minor, major, deadly = minor / all_accidents, major / all_accidents, deadly_accidents / all_accidents
        # define data
        data = [minor, major, deadly]
        labels = ['Minor injuries', 'Major injuries', 'Deadly injuries']
        # define Seaborn color palette to use
        colors = sns.color_palette('pastel')[0:3]
        # create pie chart
        plt.pie(data, labels=labels, colors=colors, autopct='%.0f%%')
        plt.show()

    def plot_death_probabilities_by_participant(self, target_file_name:str = None):
        histo = {}
        for participant in self.participants_list:
            prob = self.get_death_probability_given_participant_type(participant)
            name = str(participant).replace('BooleanField: Participants.', '')[1:-1].capitalize()
            histo[name] = prob
        df = pd.DataFrame(list(histo.items()), columns=['Category', 'Frequency'])
        plt.figure(figsize=(8, 6))
        sns.barplot(x='Category', y='Frequency', data=df, palette='muted')
        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)

    def plot_death_probabilities_by_street_type(self, target_file_name:str = None):
        street_types_osm = ['motorway', 'tertiary', 'secondary', 'primary', 'trunk', 'service', 'living_street', 'living_street']
        histo = {}
        for street in street_types_osm:
            prob = self.get_death_probability_given_street_type(street)
            histo[street.capitalize()] = prob
        df = pd.DataFrame(list(histo.items()), columns=['Category', 'Frequency'])
        plt.figure(figsize=(8, 6))
        sns.barplot(x='Category', y='Frequency', data=df, palette='muted')
        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)

    def get_death_probability_given_street_type(self, street_type: str):
        """ Assuming that an accident happens: what is the probability that at least one participant dies,
            given a road type. """
        num_all = Accident.select().where(Accident.road_type_osm == street_type).count()  # noqa: E712
        num_deadly = Accident.select().where((Accident.road_type_osm == street_type) & (Accident.severeness == 0)).count()   # noqa: E712
        probability = num_deadly / num_all
        assert probability <= 1.0 and probability >= 0.0
        return probability * 100

    def get_death_probability_given_participant_type(self, participant_type: peewee.FieldAccessor):
        """ Assuming that an accident happens: what is the probability that at least one participant dies """
        # (accidents involing participant and deadly injuries) / (all accidents involving participant type)
        num_all = Accident.select().join(Participants, on=(Accident.involved == Participants.id)) \
                                   .where(participant_type == True).count()  # noqa: E712
        num_deadly = Accident.select().join(Participants, on=(Accident.involved == Participants.id)) \
                                     .where((participant_type == True) & (Accident.severeness == 0)).count()   # noqa: E712
        probability = num_deadly / num_all
        assert probability <= 1.0 and probability >= 0.0
        return probability * 100

    def plot_avg_lighting_conditions_over_time(self):
        # do more accidents happen during night on Landstrassen (primary roads)? did it change over time?
        # loop over accidents by time
        years = range(2016, 2023)
        light_over_months = {}
        for y in years:
            months = range(1, 13)
            for m in months:
                # let avg be calculated by sql, b.c. it's wayyy faster
                light = Accident.select(fn.avg(Accident.lighting_conditions)) \
                                   .where((Accident.year == y) & (Accident.month == m)
                                          & (Accident.road_type_osm == 'primary')
                                          )
                average = light.scalar()
                light_over_months[f'{m}.{y}'] = average

        # plot to see if lighting conditions improved over time
        # Create a DataFrame from the dictionary
        df = pd.DataFrame(list(light_over_months.items()), columns=['Time', 'Value'])

        # Convert the 'Time' column to datetime type
        # df['Time'] = pd.to_datetime(df['Time'])

        # Plotting the data over time using seaborn
        plt.figure(figsize=(10, 6))
        sns.lineplot(x='Time', y='Value', data=df)
        ax = plt.gca()  # get current axis
        ax.set_xticks(df['Time'])
        ax.set_xticklabels(df['Time'], rotation=45, ha='right')
        plt.text(0.01, 0.5, '0: Daylight\n2: Dark', transform=ax.transAxes, fontsize=12)
        plt.xlabel('Time')
        plt.ylabel('Value')
        plt.title('Average Lighting Conditions over months')
        plt.tight_layout()
        plt.show()


    def plot_accidents_on_primary_loads_with_bad_lighting_conditions(self):
        pass
        # TODO: plot them make and make clusters

    def get_number_of_accidents_involving_more_than_two_different_parties(self):
        # number expect to be so low such that it can be ignored
        return (Accident
                .select()
                .join(Participants, on=(Accident.involved == Participants.id))
                .where((
                      (Participants.predestrian == True) +  # noqa: E712
                      (Participants.car == True) +  # noqa: E712
                      (Participants.bike == True) +  # noqa: E712
                      (Participants.truck == True) +  # noqa: E712
                      (Participants.other == True) +  # noqa: E712
                      (Participants.motorcycle == True)  # noqa: E712
                      ) > 2
                    )
                ).count()

    def plot_accidents_by_participants(self):
        """ Heatmap showing the frequency of pairs of participants
            involved in accidents.
        """
        def get_exclusive_count(p1, p2):
            # accidents where exclusively the given participants are involved and no others
            # e.g. accident between car and car

            l_ps = self.participants_list.copy()
            l_ps = [x for x in l_ps if x.column_name != p1.column_name]

            if p1 is not p2:
                l_ps = [x for x in l_ps if x.column_name != p2.column_name]
                p = Participants.select().where((p1 == True) & (p2 == True) &  # noqa: E712, W504
                                                (l_ps[0] == False) & (l_ps[1] == False) &  # noqa: E712, W504
                                                (l_ps[2] == False) & (l_ps[3] == False))  # noqa: E712
                assert len(p) < 2
                if len(p) == 0:
                    return 0
                p = p[0]
                r = (Accident
                     .select()
                     .where(Accident.involved == p.id)  # noqa: E712
                     .count()
                     )
                return r
            else:
                p = Participants.select().where((p1 == True) & (l_ps[4] == False) &  # noqa: E712, W504
                                                (l_ps[0] == False) & (l_ps[1] == False) &  # noqa: E712, W504
                                                (l_ps[2] == False) & (l_ps[3] == False))  # noqa: E712
                assert len(p) < 2
                if len(p) == 0:
                    return 0
                p = p[0]
                r = (Accident
                     .select()
                     .where(Accident.involved == p.id)  # noqa: E712
                     .count()
                     )
                return r

        def get_joint_count(p1, p2):
            # accidents where at least the given participants are involved but also others may be involved
            result = (Accident
                      .select()
                      .join(Participants, on=(Accident.involved == Participants.id))
                      .where((p1 == True) & (p2 == True))  # noqa: E712
                      .count()
                      )
            return result
        # number of accidents where predestrians are involved
        participants_list = [Participants.predestrian, Participants.truck,
                             Participants.motorcycle, Participants.bike,
                             Participants.car, Participants.other]
        data = {
            'Predestrian': [get_exclusive_count(participants_list[0], participants_list[0]),
                            get_exclusive_count(participants_list[0], participants_list[1]),
                            get_exclusive_count(participants_list[0], participants_list[2]),
                            get_exclusive_count(participants_list[0], participants_list[3]),
                            get_exclusive_count(participants_list[0], participants_list[4]),
                            get_exclusive_count(participants_list[0], participants_list[5])],
            'Truck':       [get_exclusive_count(participants_list[1], participants_list[0]),
                            get_exclusive_count(participants_list[1], participants_list[1]),
                            get_exclusive_count(participants_list[1], participants_list[2]),
                            get_exclusive_count(participants_list[1], participants_list[3]),
                            get_exclusive_count(participants_list[1], participants_list[4]),
                            get_exclusive_count(participants_list[1], participants_list[5])],
            'Motorcycle':  [get_exclusive_count(participants_list[2], participants_list[0]),
                            get_exclusive_count(participants_list[2], participants_list[1]),
                            get_exclusive_count(participants_list[2], participants_list[2]),
                            get_exclusive_count(participants_list[2], participants_list[3]),
                            get_exclusive_count(participants_list[2], participants_list[4]),
                            get_exclusive_count(participants_list[2], participants_list[5])],
            'Bike':        [get_exclusive_count(participants_list[3], participants_list[0]),
                            get_exclusive_count(participants_list[3], participants_list[1]),
                            get_exclusive_count(participants_list[3], participants_list[2]),
                            get_exclusive_count(participants_list[3], participants_list[3]),
                            get_exclusive_count(participants_list[3], participants_list[4]),
                            get_exclusive_count(participants_list[3], participants_list[5])],
            'Car':         [get_exclusive_count(participants_list[4], participants_list[0]),
                            get_exclusive_count(participants_list[4], participants_list[1]),
                            get_exclusive_count(participants_list[4], participants_list[2]),
                            get_exclusive_count(participants_list[4], participants_list[3]),
                            get_exclusive_count(participants_list[4], participants_list[4]),
                            get_exclusive_count(participants_list[4], participants_list[5])],
            'Other':       [get_exclusive_count(participants_list[5], participants_list[0]),
                            get_exclusive_count(participants_list[5], participants_list[1]),
                            get_exclusive_count(participants_list[5], participants_list[2]),
                            get_exclusive_count(participants_list[5], participants_list[3]),
                            get_exclusive_count(participants_list[5], participants_list[4]),
                            get_exclusive_count(participants_list[5], participants_list[5])],
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
        """ Plotting histograms of accident frequency on respective week days. """

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
