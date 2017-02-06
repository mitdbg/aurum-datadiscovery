import nltk
import re


def pos_tag_text(text):
    tagged = nltk.tag.pos_tag(text.split())
    return tagged


def get_word_with_pos(text, type):
    tagged = pos_tag_text(text)
    words = [w for w, pos in tagged if pos == type]
    return words


def get_nouns(text):
    nouns = get_word_with_pos(text, 'NN')
    return nouns


def get_proper_nouns(text):
    pnouns = get_word_with_pos(text, 'NNP')
    return pnouns


def camelcase_to_snakecase(term):
    tmp = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', term)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', tmp).lower()


def tokenize_property(prop):
    snake = camelcase_to_snakecase(prop)
    snake = snake.replace('_', ' ')
    snake = snake.replace('-', ' ')
    tokens = snake.split(' ')
    return tokens

if __name__ == "__main__":
    print("NLP Utils")
