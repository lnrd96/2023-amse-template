import pandas as pd
import numpy as np
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import peewee
import os

from database.model import Accident, Participants, Coordinate
from typing import Tuple
from tqdm import tqdm
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

        query = Accident.select(Coordinate.wsg_long, Coordinate.wsg_lat, Accident.hour, Accident.road_type_osm, Accident.lighting_conditions, Accident.road_state).join(Coordinate).order_by(fn.Random()).limit(n_accidents) #, on=(Accident.location==Coordinate.id))
        # query = Accident.select().order_by(fn.Random()).limit(n_accidents).join(Coordinate) #, on=(Accident.location==Coordinate.id))
        self._plot_query_on_map(query)
        return
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
        plt.figure(figsize=(16, 12))
        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)

    def _plot_query_on_map(self, query: peewee.ModelSelect, title='Map of Accidents'):
        """ Plots accidents present in query on a map of germany.
        """
        import plotly.io as pio
        import plotly.express as px

        pio.renderers.default = "notebook"
        df = pd.DataFrame.from_records(list(query.dicts()))
        fig = px.scatter_mapbox(df,
                                lat="wsg_lat",
                                lon="wsg_long",
                                hover_name="road_type_osm",
                                hover_data=["road_state", "lighting_conditions", "hour"],
                                color="road_type_osm",
                                zoom=5,
                                height=800,
                                width=1200)
        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        fig.show()
        return


        # plot germany
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        germany = world[world['name'] == 'Germany']
        fig, ax = plt.subplots(1, 1)
        germany.plot(ax=ax, color='white', edgecolor='black')

        # plot coordinates

        for accident in tqdm(query):
            longitude = accident.location.wsg_long
            latitude = accident.location.wsg_lat

            # TODO: distinguish between road types
            ax.plot(longitude, latitude, 'ro', markersize=1)  # 'ro' means red color, circle marker

        ax.set_title(title)
        ax.axis('off')
        plt.figure(figsize=(16, 12))  # adjust for the size of figure
        plt.show()

    def _plot_two_queries_on_map(self, query_a: peewee.ModelSelect, query_b: peewee.ModelSelect, titles: Tuple[str, str]):
        """ Plots accidents present in the queries on different maps of germany, next to each other.
            Also adds a reference map with cities and highways on it.
        """
        if 'project' not in os.getcwd():
            os.chdir(os.path.join('2023-amse-template', 'project'))

        # plot germany
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        germany = world[world['name'] == 'Germany']
        fig, axes = plt.subplots(1, 3)
        germany.plot(ax=axes[0], color='white', edgecolor='black')
        germany.plot(ax=axes[1], color='white', edgecolor='black')

        map_image = mpimg.imread('database/germany.jpg')
        axes[2].imshow(map_image, extent=[0, map_image.shape[1], 0, map_image.shape[0]])
        axes[2].axis('off')
        axes[2].set_title('Reference Map')

        # plot coordinates

        for accident in tqdm(query_a):
            longitude = accident.location.wsg_long
            latitude = accident.location.wsg_lat

            axes[0].plot(longitude, latitude, 'ro', markersize=1)  # 'ro' means red color, circle marker

        axes[0].set_title(titles[0])
        axes[0].axis('off')

        for accident in tqdm(query_b):
            longitude = accident.location.wsg_long
            latitude = accident.location.wsg_lat

            axes[1].plot(longitude, latitude, 'ro', markersize=1)  # 'ro' means red color, circle marker

        axes[1].set_title(titles[1])
        axes[1].axis('off')

        plt.figure(figsize=(16, 12))  # adjust for the size of figure
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
        explode = (0, 0, 0.1) 
        plt.pie(data, labels=labels, colors=colors, autopct='%.0f%%', explode=explode)
        plt.show()

    def plot_death_probabilities_by_participant(self, target_file_name: str = None):
        """ Bar plot showing the death frequency by involved participants. """
        histo = {}
        for participant in self.participants_list:
            prob = self._get_death_probability_given_participant_type(participant)
            name = str(participant).replace('BooleanField: Participants.', '')[1:-1].capitalize()
            histo[name] = prob
        df = pd.DataFrame(list(histo.items()), columns=['Category', 'Frequency'])
        plt.figure(figsize=(8, 6))
        sns.barplot(x='Category', y='Frequency', data=df, palette='muted')
        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)

    def plot_death_probabilities_by_street_type(self, target_file_name: str = None):
        """ Bar plot showing the death probability by selected OSM street types. """
        street_types_osm = ['motorway', 'tertiary', 'secondary', 'primary', 'trunk', 'service', 'living_street', 'living_street']
        histo = {}
        for street in street_types_osm:
            prob = self._get_death_probability_given_street_type(street)
            histo[street.capitalize()] = prob
        df = pd.DataFrame(list(histo.items()), columns=['Category', 'Frequency'])
        plt.figure(figsize=(8, 6))
        sns.barplot(x='Category', y='Frequency', data=df, palette='muted')
        if target_file_name is None:
            plt.show()
        else:
            plt.savefig(target_file_name)

    def _get_death_probability_given_street_type(self, street_type: str):
        """ Assuming that an accident happens: what is the probability that at least one participant dies,
            given a road type. """
        num_all = Accident.select().where(Accident.road_type_osm == street_type).count()  # noqa: E712
        num_deadly = Accident.select().where((Accident.road_type_osm == street_type) & (Accident.severeness == 0)).count()   # noqa: E712
        probability = num_deadly / num_all
        assert probability <= 1.0 and probability >= 0.0
        return probability * 100

    def _get_death_probability_given_participant_type(self, participant_type: peewee.FieldAccessor):
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
        years = range(2018, 2020)
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

    def plot_car_severe_and_deadly_accidents_on_highways_and_on_secondary_roads_in_the_dark(self):
        """ Map plot of severe and deadly car accidents in the dark. Respective maps for highway and secondary road. """
        only_cars = Participants.select().where((Participants.car == True) &  # noqa: E712
                                                (Participants.predestrian == False) &  # noqa: E712
                                                (Participants.bike == False) &  # noqa: E712
                                                (Participants.truck == False) &  # noqa: E712
                                                (Participants.other == False) &  # noqa: E712
                                                (Participants.motorcycle == False))  # noqa: E712
        query_a = Accident.select().where((Accident.road_type_osm == 'secondary')
                                          & (Accident.severeness < 2) & (Accident.lighting_conditions == 2)
                                          & (Accident.involved == only_cars[0].id))
        query_b = Accident.select().where((Accident.road_type_osm == 'motorway')
                                          & (Accident.severeness < 2) & (Accident.lighting_conditions == 2)
                                          & (Accident.involved == only_cars[0].id))
        self._plot_two_queries_on_map(query_a, query_b, ['Secondary Roads', 'Motorways'])

    def _get_number_of_accidents_involving_more_than_two_different_parties(self):
        """ An sql query for the number of accidents where more than two different types of participants was involved. """
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
        """ Heatmap showing the frequency of pairs of participants involved in accidents.
            It reflects how frequent accidents with different combinations of participants are.
            Accidents where more than two different types of participants are involved are not conducted. """

        def get_exclusive_count(p1, p2):
            """ Helper: accidents where exclusively the given participants are involved and no others
                        e.g. accident between car and car """

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
        plt.figure(figsize=(8, 6))
        # mask for the upper triangle, to remove redundancy resulting out of symmetry
        mask = np.triu(np.ones_like(df, dtype=bool))
        mask[np.diag_indices(mask.shape[0])] = 0  # diagonal should not be removed
        sns.heatmap(df, annot=True, mask=mask, cmap='coolwarm', fmt='d', vmax=200000, vmin=700)
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
        # df_parsed = pd.DataFrame(list(histo_dict_parsed.items()), columns=['Category', 'Frequency'])
        df_osm = pd.DataFrame(list(histo_dict_osm.items()), columns=['Category', 'Frequency'])

        fig, axes = plt.subplots(nrows=1, ncols=1)

        # sns.barplot(x='Category', y='Frequency', data=df_parsed, palette='viridis', ax=axes[0])
        # axes[0].set_title('Road type parsed out of street name')
        sns.barplot(x='Category', y='Frequency', data=df_osm, palette='viridis', ax=axes)
        axes.set_title('Road type by Open Street Map (OSM)')
        plt.setp(axes.xaxis.get_majorticklabels(), rotation=90)
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
