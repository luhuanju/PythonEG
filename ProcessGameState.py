import math
import os

import pandas as pd
import json


class ProcessGameState:
    def __init__(self, file_path, boundary):
        # read the data source from the pickle file
        self.game_state_data = self.load_file(file_path)
        self.boundaries = boundary
        self.counter = 0
        print(self.game_state_data)

    # load file by different file extension
    def load_file(self, file_path):
        split_file_name = os.path.splitext(file_path)
        # get the file extension
        if split_file_name[-1] == '.pickle':
            return pd.read_pickle(file_path)
        elif split_file_name[-1] == '.parquet':
            return pd.read_parquet(file_path)
        else:
            return None

    def filter_ETL(self):
        # filter some rows that does not have the column 'inventory'
        self.game_state_data = self.game_state_data.dropna(subset=['inventory'])

    def get_weapon_class(self):
        unique_weapon_classes = set()
        for row in self.game_state_data.itertuples(index=False):
            inven = row.inventory
            for item in inven:
                unique_weapon_classes.add(item['weapon_class'])
        return unique_weapon_classes

    def check_boundary_for_eachrow(self, row):
        z_axis_boundaries = self.boundaries[0]
        xy_axis_boundaries = self.boundaries[1]
        # check z-axis
        if not (z_axis_boundaries[0] <= row.z <= z_axis_boundaries[1]):
            # row['inside_boundary'] = False
            return False
        inside = self.is_inside_polygon(row.x, row.y, xy_axis_boundaries)
        if inside:
            self.counter += 1
        return inside
        # row['inside_boundary'] = inside

    def has_rifle_and_smg(self,inventory):
        return sum(1 for item in inventory if item['weapon_class'] in ['Rifle', 'SMG'])

    # return any(item['weapon_class'] == 'Rifle' for item in inventory) and any(item['weapon_class'] == 'Pistols' for item in inventory)


    def setWeapons(self):
        self.game_state_data['weapons'] = self.game_state_data['inventory'].apply(
            lambda inv: [item['weapon_class'] for item in inv if 'weapon_class' in item])

    def is_inside_polygon(self, x, y, vertices):
        n = len(vertices)
        inside = False
        p1x, p1y = vertices[0]
        for i in range(n + 1):
            p2x, p2y = vertices[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        return inside

    def filter_team2_valid(self, team, side):
        return self.game_state_data[
            (self.game_state_data['team'] == team) &
            (self.game_state_data['side'] == side)]

    def filter_team2_boundary_valid(self, data):
        return data[(data['inside_boundary'] == True)]

    def ave_timer(self, data):
        count = 0
        for _, row in data.iterrows():
            if row['area_name'] != 'BombsiteB':
                continue
            inventory = row['inventory']
            rifles_smgs = 0
            for item in inventory:
                if (item['weapon_class'] == 'Rifle') or (item['weapon_class'] == 'SMG'):
                    rifles_smgs += 1
            if rifles_smgs >= 2:
                count += 1
        return count

# define the boundary, including z-axis, and xy-axis based on given location
xy_axis_boundaries = [(-1735, 250), (-2806, 742), (-2472, 1233), (-1565, 580)]
z_axis_boundaries = [285, 421]
boundaries = [z_axis_boundaries, xy_axis_boundaries]

dataPath = "game_state_frame_data.pickle"
game = ProcessGameState(dataPath, boundaries)
print(game.game_state_data)
# 1 a): Handle file ingestion and ETL (if deemed necessary)
game.filter_ETL()
print('After filter_ETL--:')
print(game.game_state_data)

game.setWeapons()

# 1 b): Return whether each row falls within a provided boundary
print('check if each row falls within boundary --:')
game.game_state_data['inside_boundary'] = game.game_state_data.apply(game.check_boundary_for_eachrow, axis=1)
print(game.game_state_data)
print(game.counter)

# 1 c): Extract the weapon classes from the inventory json column
weapons_classes = game.get_weapon_class()
print(weapons_classes)

# 2 a):Is entering via the light blue boundary a common strategy used by Team2 on T (terrorist) side?
print('Team2 with I side in boundary---:')

team2_data = game.game_state_data[
    (game.game_state_data['team'] == 'Team2') & (game.game_state_data['side'] == 'T')]
# print(team2_data)

team2_data_inside_boundary = team2_data[
    (team2_data['inside_boundary'] == True)]
print("Total " + str(team2_data_inside_boundary.shape[0]) + " inside light blue  boundary")

# 2 b): What is the average timer that Team2 on T (terrorist) side enters “BombsiteB” with least 2 rifles or SMGs?
team2_BombsiteB = team2_data[(team2_data['area_name'] == 'BombsiteB')]
# calculate the count of the team2 with side t at BombsiteB, then find some rows at least 2 rifles or SMGs based on team2_BombsiteB
team2_BombsiteB_with_2rifles_or_SMGs=team2_BombsiteB[team2_BombsiteB['inventory'].apply(game.has_rifle_and_smg) >= 2]
print(team2_BombsiteB_with_2rifles_or_SMGs)
