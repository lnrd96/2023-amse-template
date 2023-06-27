import numpy as np
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
import peewee
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer
from typing import Tuple


class Analyzer():
    def __init__(self):
        pass

    def accident_probability_given_joint_probabilities(self):
        pass

    def determine_optimal_number_of_clusters_using_gap_statistics(self, query: peewee.ModelSelect, cluster_range: Tuple[int, int]):
        """ Generate an ellbow-plot to find optimal number of clusters.

        Args:
            query (peewee.ModelSelect): A query from the Coordinate relation.
            cluster_range (Tuple): Range to consider.
        """
        X = np.array([(c.wsg_long, c.wsg_lat) for c in query])
        kmeans = KMeans()
        visualizer = KElbowVisualizer(kmeans, k=cluster_range)
        visualizer.fit(X)
        visualizer.show()

    def get_k_means_from_query(self, n_centers: int, query: peewee.ModelSelect, title='', markersize=1, ext_ax=None):
        """ Calculates clusters using k-means algorithm and plots them on a map.

        Args:
            n_centers (int): Number of clusters to use.
            query (peewee.ModelSelect): The data. Must be a query from the Coordinate relation.
            title (str, optional): Title for the plot. Defaults to ''.
            markersize (int, optional): Size of accidents on map. Defaults to 1.
            ext_ax (_type_, optional): An external matplotlib axes object to use. 
                                       If not provided this function creates dedicated plot itself.
                                       Defaults to None.
        """
        X = np.array([(c.wsg_long, c.wsg_lat) for c in query])
        kmeans = KMeans(n_clusters=n_centers)
        kmeans.fit(X)
        labels = kmeans.labels_
        centers = kmeans.cluster_centers_

        # plot germany
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        germany = world[world['name'] == 'Germany']

        if ext_ax is not None:
            ax = ext_ax
        else:
            fig, ax = plt.subplots(1, 1)

        germany.plot(ax=ax, color='white', edgecolor='black')

        colors = sns.color_palette('tab10', n_centers)
        for i, c in enumerate(X):
            ax.plot(c[0], c[1], color=colors[labels[i]], marker='o', markersize=markersize)
        for i, c in enumerate(centers):
            ax.plot(c[0], c[1], marker='X', color='black', markersize=5, zorder=10)

        ax.set_title(title)
        ax.axis('off')
        if ext_ax is None:
            plt.figure(figsize=(8, 6))
            plt.show()
