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
        if not self.memos_to_categories_dict:
            self.refresh_memos_to_cateogries_dict()

        if memo in self.memos_to_categories_dict:
            return self.memos_to_categories_dict[memo]

        # Else, might need to do some keyword searching to make a best guess at the category


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


