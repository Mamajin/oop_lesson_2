import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

players = []
with open(os.path.join(__location__, 'PLayers.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)


# player, teams and titanic part
table1 = Table('players', players)
table2 = Table('teams', teams)
table3 = Table('titanic', titanic)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
print("What player on a team with “ia” in the team name played less than \n"
      "200 minutes and made more than 100 passes? Select to display the \n"
      "player surname, team, and position.")
my_table1 = my_DB.search('players')
my_table1_filtered = my_table1.filter(lambda x: 'ai' in x['team'])
my_table1_filtered = my_table1_filtered.filter(lambda x: int(x['minutes']) < 200)
my_table1_filtered = my_table1_filtered.filter(lambda x: int(x['passes']) > 100)
my_table1_selected = my_table1_filtered.select(['surname', 'team', 'position'])
print(my_table1_selected)
print()

print("The average number of games played for teams ranking below 10 versus \n"
      "teams ranking above or equal 10")
my_table2 = my_DB.search('teams')
rank_below10 = my_table2.filter(lambda x: int(x['ranking']) < 10).aggregate(lambda x: sum(x)/len(x), 'games')
rank_above_or_eq10 = my_table2.filter(lambda x: int(x['ranking']) >= 10).aggregate(lambda x: sum(x)/len(x), 'games')
print(f"{rank_below10} vs {rank_above_or_eq10}")
print()

print("The average number of passes made by forwards versus by midfielders")
my_table1_forwards = my_table1.filter(lambda x: x['position'] == 'forward').aggregate(lambda x: sum(x)/len(x), 'passes')
my_table1_mid = my_table1.filter(lambda x: x['position'] == 'midfielder').aggregate(lambda x: sum(x)/len(x), 'passes')
print(f"{my_table1_forwards} vs {my_table1_mid}")
print()


# titanic section
# dataset's key last,first,gender,age,class,fare,embarked,survived
my_table3 = my_DB.search('titanic')
print("The average fare paid by passengers in the first class versus \n"
      "in the third class")
first_avg = my_table3.filter(lambda x: int(x['class']) == 1).aggregate(lambda x: sum(x)/len(x), 'fare')
third_avg = my_table3.filter(lambda x: int(x['class']) == 3).aggregate(lambda x: sum(x)/len(x), 'fare')
print(f"{first_avg} vs {third_avg}")
print()

print("The survival rate of male versus female passengers")
male_total = len(my_table3.filter(lambda x: x['gender'] == 'M').table)
male_survive = len(my_table3.filter(lambda x: x['gender'] == 'M').filter(lambda x: x['survived'] == 'yes').table)
male_rate = male_survive / male_total * 100
female_total = len(my_table3.filter(lambda x: x['gender'] == 'F').table)
female_survive = len(my_table3.filter(lambda x: x['gender'] == 'F').filter(lambda x: x['survived'] == 'yes').table)
female_rate = female_survive / female_total * 100
print(f"{male_rate:.2f}% vs {female_rate:.2f}%")
print()

print("Total number of male passengers embarked at Southampton")
m_embarked_south = len(my_table3.filter(lambda x: x['gender'] == 'M').filter(lambda x: x['embarked'] == 'Southampton').table)
print(m_embarked_south)
# table1 = Table('cities', cities)
# table2 = Table('countries', countries)
# my_DB = DB()
# my_DB.insert(table1)
# my_DB.insert(table2)
# my_table1 = my_DB.search('cities')
# print(my_table1)
#
# print("Test filter: only filtering out cities in Italy")
# my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
# print(my_table1_filtered)
# print()
#
# print("Test select: only displaying two fields, city and latitude, for cities in Italy")
# my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
# print(my_table1_selected)
# print()
#
# print("Calculting the average temperature without using aggregate for cities in Italy")
# temps = []
# for item in my_table1_filtered.table:
#     temps.append(float(item['temperature']))
# print(sum(temps)/len(temps))
# print()
#
# print("Calculting the average temperature using aggregate for cities in Italy")
# print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
# print()
#
# print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
# my_table2 = my_DB.search('countries')
# my_table3 = my_table1.join(my_table2, 'country')
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
# print(my_table3_filtered.table)
# print()
# print("Selecting just three fields, city, country, and temperature")
# print(my_table3_filtered.select(['city', 'country', 'temperature']))
# print()
#
# print("Print the min and max temperatures for cities in EU that do not have coastlines")
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
# print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
# print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
# print()
#
# print("Print the min and max latitude for cities in every country")
# for item in my_table2.table:
#     my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
#     if len(my_table1_filtered.table) >= 1:
#         print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
# print()
