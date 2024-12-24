import requests
import time
import re
import pickle as pkl

import pandas as pd

from bs4 import BeautifulSoup as bs
from bs4 import Comment
from IPython.display import clear_output

class PlayerScraper():
    def __init__(self, save_csv=True):
        self.save_csv = save_csv

    def scrape_player_info(self, leading_letters = ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                                         'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                                         'U', 'V', 'W', 'X', 'Y', 'Z')):

        # Create storage
        names = []
        suffixes = []
        ids = []

        for letter in leading_letters:
            # Sleep to remain under rate limits
            time.sleep(4)

            # Create the url with the data for all players with a last name starting with the letter
            letter_url = f'https://www.baseball-reference.com/players/{letter.lower()}/'
            
            # Request the url that contains the data for all players belonging to the letter
            req = requests.get(letter_url)
            soup = bs(req.text, 'html.parser')

            player_soup = soup.find_all('div', {'id':'div_players_'})[0]

            # Grab all names
            player_names = [name.a.text for name in player_soup.find_all('p')]
            names = names + player_names

            # Grab all URL suffixes
            url_suffixes = [name.a['href'] for name in player_soup.find_all('p')]
            suffixes = suffixes + url_suffixes

            # Grab IDs
            player_ids = [name.a['href'].split('/')[-1].split('.sh')[0] for name in player_soup.find_all('p')]
            ids = ids + player_ids

        # Combine everything into a dataframe
        player_df = pd.DataFrame({'name':names, 'id':ids, 'url_suffix':suffixes})

        if self.save_csv:
            with open('../../data/raw-data/all_player_info.csv', 'w') as file:
                player_df.to_csv(file, index=False)

    def _scrape_career_batting_or_pitching_stats_from_soup(self, position: str, soup: bs):
        if position.lower() not in ['pitching', 'batting', 'fielding']:
            raise ValueError(f"Position {position} is invalid: Must be one of 'batting' or 'pitching' or 'fielding'")

        is_position = True if soup.find_all('div', {'id':f'all_players_standard_{position}'}) else False

        if is_position:
            try: # Pull the career standard stats
                standard_footer = soup.find_all('div', {'id':f'all_players_standard_{position}'})[0].find('tfoot').find('tr', {'id':re.compile(f'players_standard_{position}.')})
            except (IndexError, AttributeError) as e: # If the table is commented out
                comments = soup.find_all(string=lambda text: isinstance(text, Comment))
                for comment in comments: # Iterate through the comments
                    if f'players_standard_{position}' in comment:
                        table_html = bs(comment, 'html.parser')
                        standard_footer = table_html.find_all('table', {'id':f'players_standard_{position}'})[0].find('tfoot').find('tr', {'id':re.compile(f'players_standard_{position}.')})
                        break
            # Combine the stat names and values into lists
            stat_names = [cell['data-stat'] for cell in standard_footer.find_all('td')]
            stat_values = [cell.text for cell in standard_footer.find_all('td')]
            career_standard_stats = pd.DataFrame([stat_values], columns=stat_names)

            if position.lower() != 'fielding':
                try: # Pull the career advanced stats
                    advanced_footer = soup.find_all('div', {'id':f'all_players_advanced_{position}'})[0].find('tfoot').find('tr', {'id':re.compile(f'players_advanced_{position}.')})
                except (IndexError, AttributeError) as e: # If the table is commented out
                    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
                    for comment in comments: # Iterate through the comments
                        if f'players_advanced_{position}' in comment:
                            table_html = bs(comment, 'html.parser')
                            advanced_footer = table_html.find_all('table', {'id':f'players_advanced_{position}'})[0].find('tfoot').find('tr', {'id':re.compile(f'players_advanced_{position}.')})
                            break
                # Combine the stat names and values into lists
                stat_names = [cell['data-stat'] for cell in advanced_footer.find_all('td')]
                stat_values = [cell.text for cell in advanced_footer.find_all('td')]
                career_advanced_stats = pd.DataFrame([stat_values], columns=stat_names)

                total_career_stats = pd.concat([career_standard_stats, career_advanced_stats], axis=1)

                return total_career_stats
            
            else:
                return career_standard_stats
        
        else: # If no data for the position
            return pd.DataFrame()
    
    def _scrape_annual_batting_or_pitching_stats_from_soup(self, position, level, soup):

        if position.lower() not in ['pitching', 'batting', 'fielding']:
            raise ValueError(f"Position {position} is invalid: Must be one of 'batting' or 'pitching' or 'fielding")
        
        # Check if the player is the right position
        is_position = True if soup.find_all('div', {'id':f'all_players_standard_{position}'}) else False

        if is_position:
            try: # Pull the stats as standard
                rows = soup.find_all('tr', {'id':re.compile(f'players_{level}_{position}.')})
                existince_checker = rows[0]
            except (IndexError, AttributeError) as e: # If the table is commented out
                comments = soup.find_all(string=lambda text: isinstance(text, Comment))
                for comment in comments: # Iterate through the comments
                    if f'players_{level}_{position}' in comment:
                        table_html = bs(comment, 'html.parser')
                        rows = table_html.find_all('table', {'id':f'players_{level}_{position}'})[0].find('tbody').find_all('tr', {'id':re.compile(f'players_{level}_{position}.')})
                        break
            
            # Build the stats and values into lists
            headers = [cell['data-stat'] for cell in rows[0].find_all('td')]

            stats_list = []
            for row in rows:
                stats = [cell.text for cell in row.find_all('td')]
                stats_list.append(stats)
            stats = pd.DataFrame(stats_list, columns=headers)
            stats = stats[stats[stats.columns[-2]] != None]

            return stats

    def _scrape_acomplishments_from_soup(self, soup):
        try: # Pull the awards
            accomplishments_soup = soup.find_all('ul',{'id':'bling'})[0]
            accomplishments = [accomplishment.text for accomplishment in accomplishments_soup.find_all('li')]
            accomplishments = ', '.join(accomplishments)
            return accomplishments
        except (AttributeError, IndexError) as e: # When there are no awards present
            return ''

    def _scrape_position_appearances_from_soup(self, soup):
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments: # Iterate through the comments
            if f'div_appearances' in comment:
                table_html = bs(comment, 'html.parser')
                footer_row = table_html.find('tfoot')
                headers = [cell['data-stat'] for cell in footer_row.find_all('td')]
                stats = [cell.text for cell in footer_row.find_all('td')]
                break
        app_df = pd.DataFrame([stats], columns=headers)
        return app_df

    def scrape_player_stats(self, player_suffixs = (), cache_path = ''):
        try:
            with open(cache_path, 'rb') as fpath:
                storage_dict = pkl.load(fpath)
        except FileNotFoundError:
            storage_dict = {}

        save_counter = 0
        for suffix in player_suffixs:
            insert = suffix.split('/')[-1].split('.')[0]
            if insert in storage_dict:
                continue
            
            # Wrap everything in a try/except to catch anything unforseen, while still appending to storage dict, so we can go back later if needed
            try:
                # Create the url with the data for all players with a last name starting with the letter
                player_url = f'https://www.baseball-reference.com{suffix}'
                
                # Request the url for the players Baseball Reference page
                req = requests.get(player_url)
                soup = bs(req.text, 'html.parser')

                # Scrape the career batting pitching, and fielding stats
                career_batting_stats = self._scrape_career_batting_or_pitching_stats_from_soup('batting', soup)
                career_pitching_stats = self._scrape_career_batting_or_pitching_stats_from_soup('pitching', soup)
                #career_fielding_stats = self._scrape_career_batting_or_pitching_stats_from_soup('fielding', soup)
                
                career_stats = pd.concat([career_batting_stats, career_pitching_stats], axis=1)

                # Add awards/accomplishments to the career stats
                accomplishment_list = self._scrape_acomplishments_from_soup(soup)
                career_stats['accomplishments'] = accomplishment_list

                ##### ANNUAL STATS #####

                # Scrape the annual batting, pitching, and fielding stats, combining the standard and advanced for each
                annual_standard_batting_stats = self._scrape_annual_batting_or_pitching_stats_from_soup('batting', 'standard', soup)
                annual_advanced_batting_stats = self._scrape_annual_batting_or_pitching_stats_from_soup('batting', 'advanced', soup)
                
                # Check if the batting stats existed, and if so, merge them into one df
                if isinstance(annual_standard_batting_stats, pd.DataFrame):
                    annual_batting_stats = pd.concat([annual_standard_batting_stats, annual_advanced_batting_stats], axis=1)
                    annual_batting_stats = annual_batting_stats.loc[:, ~annual_batting_stats.columns.duplicated()]
                else:
                    annual_batting_stats = pd.DataFrame()
                # Get the index of the career column, and get rid of it and anything below (postseason)
                i = annual_batting_stats[annual_batting_stats.age.str.contains(f'\.') == True].index[0] if isinstance(annual_batting_stats, pd.DataFrame) and not annual_batting_stats.empty and len(annual_batting_stats[annual_batting_stats.age.str.contains(f'\.') == True]) > 0 else len(annual_batting_stats.index)
                annual_batting_stats = annual_batting_stats.iloc[:i]

                annual_standard_pitching_stats = self._scrape_annual_batting_or_pitching_stats_from_soup('pitching', 'standard', soup)
                annual_advanced_pitching_stats = self._scrape_annual_batting_or_pitching_stats_from_soup('pitching', 'advanced', soup)

                # Check if the pitching stats existed, and if so, merge them into one df
                if isinstance(annual_standard_pitching_stats, pd.DataFrame):
                    annual_pitching_stats = pd.concat([annual_standard_pitching_stats, annual_advanced_pitching_stats], axis=1)
                    annual_pitching_stats = annual_pitching_stats.loc[:, ~annual_pitching_stats.columns.duplicated()]
                else:
                    annual_pitching_stats = pd.DataFrame()
                # Get the index of the career column, and get rid of it and anything below (postseason)
                i = annual_pitching_stats[annual_pitching_stats.age.str.contains(f'\.') == True].index[0] if isinstance(annual_pitching_stats, pd.DataFrame) and not annual_pitching_stats.empty and len(annual_pitching_stats[annual_pitching_stats.age.str.contains(f'\.') == True]) > 0 else len(annual_pitching_stats)
                annual_pitching_stats = annual_pitching_stats.iloc[:i]

                # Merge any annual DataFrames that actually exist. Then drop any duplicated columns
                real_dfs = [df for df in [annual_batting_stats, annual_pitching_stats] if not df.empty]
                annual_stats = real_dfs[0]
                for df in real_dfs[1:]:
                    annual_stats = pd.merge(annual_stats, df, on=['age', 'team_name_abbr'], how='outer')
                annual_stats = annual_stats.loc[:, ~annual_stats.columns.duplicated()]

                # Drop rows that don't belong on known conditions
                annual_stats = annual_stats.dropna(subset=['age'])

                # Scrape appearences
                appearances = self._scrape_position_appearances_from_soup(soup)
                
                # Save everything to our storage
                storage_dict[insert] = {'career_stats':career_stats, 'annual_stats':annual_stats, 'appearances':appearances}

            except:
                storage_dict[insert] = 'FAILED TO PULL DATA'

            save_counter += 1
            if save_counter % 5 == 0:
                print(insert)
                with open('../../data/raw-data/all_player_stats.pkl', 'wb') as fpath:
                    pkl.dump(storage_dict, fpath)

            if save_counter % 200 == 0:
                print(insert)
                with open(f'../../data/raw-data/inc_saves/all_player_stats_BIGSAVE{save_counter}.pkl', 'wb') as fpath:
                    pkl.dump(storage_dict, fpath)
            

        

class HOFScraper():
    def __init__(self, save_csv=True):
        self.save_csv = save_csv

    def scrape_hof_inductees(self):
        '''Function to scrape baseball references Hall of Fame webpage, and collect information on every inducted member into the HOF.
           Optionally, saves the data as a csv.'''

        # Pull table for all players inducted to the HOF
        hof_url = 'https://www.baseball-reference.com/awards/hof.shtml'
        req = requests.get(hof_url)
        soup = bs(req.text, 'html.parser')

        ### Build out the dataframe, column by column ###

        # Year
        all_years = soup.find_all('th', {'data-stat':'year_ID'})
        years = [int(all_years[n].a.text) for n in range(1, len(all_years))]

        # Player Name
        all_players = soup.find_all('td', {'data-stat':'player'})
        players = [all_players[n].a.text for n in range(len(all_players))]

        # Living Status
        all_status = soup.find_all('td', {'data-stat':'lifespan'})
        status = [all_status[n].text for n in range(len(all_status))]

        # Entrance Method
        all_entrance_methods = soup.find_all('td', {'data-stat':'votedBy'})
        entrance_methods = [all_entrance_methods[n].text for n in range(len(all_entrance_methods))]

        # Induction Identity
        all_identities = soup.find_all('td', {'data-stat':'category_hof'})
        identities = [all_identities[n].text for n in range(len(all_identities))]

        # Total Votes For, including the if statement in the list comprehension for players induction via 0-vote processes
        all_raw_votes = soup.find_all('td', {'data-stat':'votes'})
        raw_votes = [all_raw_votes[n].text if pd.isna(all_raw_votes[n]) == False else None for n in range(len(all_raw_votes))]

        # Vote Percentage, including the if statement in the list comprehension for players induction via 0-vote processes
        all_vote_percentages = soup.find_all('td', {'data-stat':'votes_pct'})
        vote_percentages = [all_vote_percentages[n].text if pd.isna(all_vote_percentages[n]) == False else None for n in range(len(all_vote_percentages))]

        # Put all of our data into a dictionary for easy conversion to a Pandas DataFrame
        conversion_dict = {'voting_year':years, 'player':players,
                        'living_status':status, 
                        'voting_body':entrance_methods,
                        'inducted_as':identities, 'votes':raw_votes,
                        'vote_percentage':vote_percentages}

        # And finally make the DataFrame
        hof_df = pd.DataFrame(conversion_dict)

        # Save as csv
        if self.save_csv:
            with open('../../data/raw-data/all_hof_inductees_table.csv', 'w') as file:
                hof_df.to_csv(file, index=False)

    def scrape_hof_voting(self, years=None):
        
        # Build the unique urls for each year's webpage
        try:
            page_urls = [f'https://www.baseball-reference.com/awards/hof_{year}.shtml' for year in years]
        except TypeError:
            raise TypeError("Must set the 'years' input of scrape_hof_voting to an iterable object of length at least one")

        # Create storage for all of our yearly HOF voting tables
        voting_tables = []

        # Iterate over each page and scrape the table
        for url in page_urls:
            # Sleep as to abide by website scraping rules
            time.sleep(4)

            # Note the given year
            year = years[page_urls.index(url)]
            print(year)

            # Gather the soup and filter down 
            req = requests.get(url)
            soup = bs(req.text, 'html.parser')

            ########## SCRAPE THE BBWA TABLE ##########
            try: # Early on, there was not voting every year, so we need to skip these 'incorrect' URLs
                bbwa_soup = soup.find_all('div', {'id':'div_hof_BBWAA'})[0]
            except IndexError:
                try: # Table named differently in 1946
                    bbwa_soup = soup.find_all('div', {'id':'div_hof_Nominating_Vote'})[0]
                except IndexError:
                    print(f'No Data for {year}')
                    pass
            
            bbwa_table = {}
            
            # Pull each column in the BBWA table and format into a list for later DF creation
            rank_boxes = bbwa_soup.find_all('th', {"data-stat":'ranker'})
            ranks = [box.text for box in rank_boxes[1:]]
            bbwa_table['rank'] = ranks

            name_boxes = bbwa_soup.find_all('td', {"data-stat":'player'})
            names = [box.a.text for box in name_boxes]
            player_page_urls = ['https://www.baseball-reference.com' + box.a['href'] for box in name_boxes]
            bbwa_table['name'] = names
            bbwa_table['player_page_url'] = player_page_urls

            # After the first two columns, everything is laid our similarly, so we can scrape in a loop
            data_stats = ['year_on_ballot', 'votes', 'votes_pct', 'hof_monitor', 'hof_standard', 'experience', 'WAR_career',
                          'WAR_peak7', 'JAWS', 'JAWS_pos', 'G', "AB", "R", 'H', 'HR', 'RBI', 'SB', 'BB', 'batting_avg',
                          'onbase_perc', 'slugging_perc', 'onbase_plus_slugging', 'onbase_plus_slugging_plus', 'W', 'L',
                          'earned_run_avg', 'earned_run_avg_plus', 'whip', 'G_p', 'GS', 'SV', 'IP', 'IP', 'H_p', 'HR_p',
                          'BB_p', 'SO_p', "pos_summary"]
            
            for stat in data_stats:
                stat_boxes = bbwa_soup.find_all('td', {"data-stat":stat})
                stats = [box.text for box in stat_boxes]

                bbwa_table[stat] = stats

            # Convert the data from the bbwa table into a pandas df, and add a column for the voting year
            bbwa_df = pd.DataFrame(bbwa_table)
            bbwa_df['voting_year'] = year

            # Append the table to the voting tables dictionary
            voting_tables.append(bbwa_df)

        # Combine all yearly voting tables into one dataframe
        hof_voting_df = pd.concat([df for df in voting_tables])

        # Optionally, save the df to the data folder
        if self.save_csv:
            with open('../../data/raw-data/yearly_hof_voting_data.csv', 'w') as file:
                hof_voting_df.to_csv(file, index=False)
