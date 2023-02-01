import logging

from Levenshtein import distance as levenshtein_distance
import re

from database.sqlite_client import SqliteClient


class Categorizer:

    def __init__(self, sqlite_client: SqliteClient):
        self.sqlite_client = sqlite_client
        self.memos_to_categories_dict = {}
        self.categories_list = []

    def clean_string(self, memo):
        # Get rid of any characters that aren't alphanumeric or spaces
        memo = re.sub('[^a-zA-Z\d\s:]', ' ', memo)
        # One more cleanup to get rid of multiple spaces in a row
        memo = re.sub('\s+', ' ', memo)

        # Lowercase to keep everything consistent
        return memo.lower()

    def guess_best_category(self, memo):
        memo = self.clean_string(memo)

        if not self.memos_to_categories_dict:
            self.refresh_memos_to_cateogries_dict()

        if memo in self.memos_to_categories_dict:
            return self.memos_to_categories_dict[memo]

        # Else, might need to do some keyword searching to make a best guess at the category
        # throwing this in code for now.  It could definitely be in the DB if it were able to be
        # customized, though.
        mappings_in_code = {
            'amzn mktp': 'amazon',
            'amazon com': 'amazon',
            'amz descriptor': 'amazon',
            'amzn': 'amazon',
            'exxonmobil': 'travel',
            'nintendo': 'recreation',
            'wf wayfair': 'home improvement',
            'target': 'target',
            'kindle svcs': 'recreation',
        }

        for key in mappings_in_code:
            if key in memo:
                logging.debug('Using mapped keyword to find category: ' + mappings_in_code[key])
                return {'category_id': None, 'category_name': mappings_in_code[key]}

        return None

    def refresh_memos_to_cateogries_dict(self):
        data = self.sqlite_client.get_memos_to_categories()

        self.memos_to_categories_dict = {}
        for datum in data:
            self.memos_to_categories_dict[datum[2]] = {'category_id': datum[0], 'category_name': datum[1]}

    def get_very_similar_category(self, input_category):
        if not self.categories_list:
            self.refresh_categories_list()

        max_accepted_distance = 3
        for item in self.categories_list:
            distance = levenshtein_distance(item['category_name'], input_category, score_cutoff=3)

            if distance <= max_accepted_distance:
                return item

        return None

    def refresh_categories_list(self):
        results = self.sqlite_client.get_categories()
        self.categories_list = []

        for result in results:
            id = result[0]
            name = result[1]

            self.categories_list.append({'category_id': id, 'category_name': name})

    def insert_category(self, best_new_category_name):
        self.sqlite_client.insert_category(best_new_category_name)
        self.refresh_categories_list()

    def make_note_of_memo_and_category(self, memo, category_id):
        if memo not in self.memos_to_categories_dict:
            self.sqlite_client.insert_memo_to_category(memo, category_id)
            self.refresh_memos_to_cateogries_dict()

    def get_category_names(self):
        if not self.categories_list:
            self.refresh_categories_list()

        return sorted([a['category_name'] for a in self.categories_list])

    def determine_category_id(self, memo):
        best_category = self.guess_best_category(memo)

        if best_category:
            category_name = best_category['category_name']
            logging.debug('Using best guess category: ' + category_name)

            if not best_category['category_id']:
                best_category['category_id'] = self.sqlite_client.get_category_id(category_name)

            return best_category['category_id']

        category_names = self.get_category_names()
        best_new_category_name = input(
            'No category on file.  Categories on file:\n%s\n\nWhat category should we use?: ' % (
                '\n'.join(category_names))).lower()

        very_similar_category = self.get_very_similar_category(best_new_category_name)
        if very_similar_category:
            very_similar_category_name = very_similar_category['category_name']

            if very_similar_category_name != best_new_category_name:
                answer = input('Did you mean: %s? (Y/N): ' % very_similar_category_name).lower()
                if answer == 'y':
                    return very_similar_category['category_id']
            else:
                return very_similar_category['category_id']

        self.insert_category(best_new_category_name)

        logging.info('Inserted new category: ' + best_new_category_name)

        return self.sqlite_client.get_category_id(best_new_category_name)

