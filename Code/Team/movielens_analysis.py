import re
from collections import defaultdict, Counter, OrderedDict
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import pytest


def read_file(path_to_the_file, count_lines=10):
    try:
        with open(path_to_the_file, "r") as file:
            next(file)
            counter = 0
            for line in file:
                counter += 1
                if counter == 1:
                    continue
                yield line
                if counter == count_lines + 1:
                    break
    except Exception as e:
        raise Exception(f"Произошла ошибка при работе с файлом {path_to_the_file}: {e}")

def read_csv_as_dict(file_path, delimiter=',', encoding='utf-8', count_lines=None, valid_movie_ids=None):

    data = []
    n1 = ['userId','movieId','rating','timestamp']
    n2 = ['userId','movieId','tag','timestamp']
    try:
        with open(file_path, 'r', encoding=encoding) as f:

            headers = [h.strip() for h in f.readline().strip().split(delimiter)]
            if headers != n1 and headers != n2:
                raise Exception("error header")
            if (len(headers)) != 4:
                raise Exception("Error")
                

            line_count = 0
            for line in f:
                if count_lines and line_count >= count_lines:
                    break
                if not line.strip():
                    continue

                values = [v.strip() for v in line.strip().split(delimiter)]
                if len(values) != len(headers):
                    continue    
                row = dict(zip(headers, values))

                # фильтрация по movieId
                if valid_movie_ids is not None:
                    try:
                        movie_id = int(row['movieId'])
                        if movie_id not in valid_movie_ids:
                            continue
                    except Exception:
                        continue  # Пропускаем если нет movieId или он не число

                data.append(row)
                line_count += 1
    except FileNotFoundError:
        print(f"Файл не найден: {file_path}")
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")

    return data

def mean(lst):
    return sum(lst) / len(lst) if lst else 0

def median(lst):
    lst = sorted(lst)
    n = len(lst)
    if n == 0:
        return 0
    mid = n // 2
    if n % 2 == 0:
        return (lst[mid - 1] + lst[mid]) / 2
    else:
        return lst[mid]

def variance(lst):
    n = len(lst)
    if n < 2:
        return 0
    avg = mean(lst)
    return sum((x - avg) ** 2 for x in lst) / (n - 1)

class Links:
    """
    Analyzing data from links.csv
    """
    def __init__(self, path_to_the_file):
        """
        Put here any fields that you think you will need.
        """
        self._path_to_the_file = path_to_the_file
        self.dict_file = Links.read_csv_column(path_to_the_file, 'imdbId')
        self.imdb_info = self.__imdb_getter()

    @staticmethod
    def read_csv_column(file_path, column_name, valid_movie_ids=None):
        values = []
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                headers = file.readline().strip().split(',')
                if headers != ["movieId", "imdbId", "tmdbId"]:
                    raise ValueError("Invalid CSV structure. Expected columns: 'movieId,imdbId,tmdbId'")
                    
                if column_name not in headers:
                    raise ValueError(f"Column '{column_name}' not found in headers.")
                if valid_movie_ids is not None and "movieId" not in headers:
                    raise ValueError("Column 'movieId' is required for validation but not found in headers.")

                column_index = headers.index(column_name)
                movie_id_index = headers.index("movieId") if valid_movie_ids is not None else None

                for line in file:
                    line = line.strip()
                    row = line.split(',')

                    # Пропуск строк с количеством колонок не равным 3
                    if len(row) != 3:
                        continue

                    if valid_movie_ids is not None:
                        try:
                            movie_id = int(row[movie_id_index])
                            if movie_id not in valid_movie_ids:
                                continue
                        except Exception:
                            continue  # Пропуск некорректного movieId



                    value = row[column_index].strip()
                    values.append(value)

        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")

        return values

    def __imdb_getter(self):
        list_of_movies = self.dict_file[:10]
        list_of_fields = ['Director', 'Budget', 'Gross worldwide', 'Gross US & Canada', 
                          'Opening weekend US & Canada', 'Runtime', 'Title']
        return Links.get_imdb(list_of_movies, list_of_fields)
    
    @staticmethod
    def __Connection(imdbId):
        url = f'https://www.imdb.com/title/tt{imdbId}/'
        headers = { 
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.imdb.com/title/',
            'Upgrade-Insecure-Requests': '1'
        }
        page = requests.get(url, headers=headers)
        if page.status_code != 200:
            raise Exception(f'Error page: {page.status_code}')
        return page
    
    @staticmethod
    def __Make_dict(soup):
        dic = {
            'Director': 'Unknown',
            'Budget': '0',
            'Gross worldwide': '0',
            'Gross US & Canada': '0',
            'Opening weekend US & Canada': '0',
            'Runtime': '0',
            'Title': '-'
        }

        # Title
        try:
            title_tag = soup.find('h1')
            if title_tag:
                dic['Title'] = title_tag.text.strip()
        except:
            pass

        # Director
        try:
            director_section = soup.find('li', {'data-testid': 'title-pc-principal-credit'})
            if director_section:
                director_link = director_section.find('a')
                if director_link:
                    dic['Director'] = director_link.text.strip()
        except:
            pass

        # Runtime
        try:
            runtime_li = soup.find('li', {'data-testid': 'title-techspec_runtime'})
            if runtime_li:
                runtime_text = runtime_li.text.strip()
                runtime_clean = runtime_text.replace("Runtime", "").strip()
                dic['Runtime'] = runtime_clean
        except:
            pass

        # Box office section
        try:
            box_office_section = soup.find_all('li', {'data-testid': 'title-boxoffice-section'})
            if not box_office_section:
                box_office_section = soup.find_all('li', {'data-testid': re.compile(r'title-boxoffice.*')})
            for li in box_office_section:
                key_span = li.find('span', string=True)
                val_span = li.find_all('span')[-1]
                if key_span and val_span:
                    key = key_span.text.strip()
                    val = val_span.text.strip()
                    if key in dic:
                        dic[key] = val
        except:
            pass

        return dic
    
    @staticmethod
    def __extract_rating(soup):
        try:
            rating_label = soup.find(string='IMDb RATING')
            if not rating_label:
                return None
            rating = rating_label.find_next().text.strip()[:6]
            return rating
        except AttributeError:
            return None

    @staticmethod
    def get_imdb(list_of_movies, list_of_fields):
        """
        The method returns a list of lists [movieId, field1, field2, field3, ...] for the list of movies given as the argument (movieId).
            For example, [movieId, Director, Budget, Cumulative Worldwide Gross, Runtime].
            The values should be parsed from the IMDB webpages of the movies.
        Sort it by movieId descendingly.
        """
        imdb_info = []
        superdict = {}
        try:
            for id in list_of_movies:
                page = Links.__Connection(id)
                soup = BeautifulSoup(page.text, "html.parser")
                superdict = Links.__Make_dict(soup)
                appended_list = [superdict[field] for field in list_of_fields]
                appended_list.insert(0,id)
                imdb_info.append(appended_list)
        except Exception as e:
            print(f"Error: {e}") 
        sorted_data = sorted(imdb_info, key=lambda x: x[0], reverse=True)
        return sorted_data
        
    def top_directors(self, n):
        """
        The method returns a dict with top-n directors where the keys are directors and 
        the values are numbers of movies created by them. Sort it by numbers descendingly.
        """
        directors = {}
        for i in self.imdb_info:
            if i[1] not in directors:
                directors[i[1]] = 1
            else:
                directors[i[1]] += 1

        return dict(sorted(directors.items(), key=lambda x: x[1], reverse=True)[:n])
        
    def most_expensive(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are their budgets. Sort it by budgets descendingly.
        """
        budgets = {}
        for i in self.imdb_info:
            budget = float(''.join(filter(str.isdigit, i[2]))) 
            budgets[i[7]] = budget

        return dict(sorted(budgets.items(), key=lambda x: x[1], reverse=True)[:n])
        
    def most_profitable(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are the difference between cumulative worldwide gross and budget.
     Sort it by the difference descendingly.
        """
        profits = {}
        for i in self.imdb_info:
            gross_worldwide = float(''.join(filter(str.isdigit, i[3])))
            budget = float(''.join(filter(str.isdigit, i[2])))
            profits[i[7]] = gross_worldwide - budget

        return dict(sorted(profits.items(), key=lambda x: x[1], reverse=True)[:n])
        
    @staticmethod
    def parse_runtime(runtime_str):
        """
        Преобразует строку вроде 'Runtime2 hours 4 minutes' или '1 hour 45 minutes' в общее количество минут.
        """
        try:
            if not runtime_str:
                return 0

            runtime_str = re.sub(r'^[^\d]*', '', runtime_str)

            minutes = 0
            match = re.search(r'(\d+)\s*hour', runtime_str)
            if match:
                minutes += int(match.group(1)) * 60
            match = re.search(r'(\d+)\s*minute', runtime_str)
            if match:
                minutes += int(match.group(1))
            return minutes
        except Exception as e:
            print(f"Runtime parse error: {e} for '{runtime_str}'")
            return 0
        
    def longest(self, n):
        """
        Returns a dict of top-n longest movies by runtime.
        Keys are movie titles, values are runtime in minutes.
        Sorted by runtime descendingly.
        """
        parsed = []
        for row in self.imdb_info:
            title = row[7]
            runtime_str = row[6]

            if not runtime_str:
                continue

            numbers = list(map(int, re.findall(r'\d+', runtime_str)))
            if not numbers:
                continue

            if 'hour' in runtime_str:
                if len(numbers) == 2:
                    total_minutes = numbers[0] * 60 + numbers[1]
                else:
                    total_minutes = numbers[0] * 60
            else:
                total_minutes = numbers[0]

            parsed.append((title, total_minutes))

        top = sorted(parsed, key=lambda x: x[1], reverse=True)[:n]
        return dict(top)
        
    def top_cost_per_minute(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and
the values are the budgets divided by their runtime. The budgets can be in different currencies – do not pay attention to it. 
     The values should be rounded to 2 decimals. Sort it by the division descendingly.
        """
        costs = {}
        for i in self.imdb_info:
            mins = int(i[6].split()[0]) * 60 + int(i[6].split()[2])
            budget = float(''.join(filter(str.isdigit, i[2])))
            costs[i[7]] = budget / mins

        return dict(sorted(costs.items(), key=lambda x: x[1], reverse=True)[:n])
    

    @staticmethod
    def get_imdb_rating(list_of_movies):
        rating_info = []

        for movie_id in list_of_movies:
            try:

                response = Links.__Connection(movie_id)
                soup = BeautifulSoup(response.text, "html.parser")

                #parsed_data = Links.__Make_dict(soup)

                title_div = soup.find('title')
                if title_div:
                    full_title = title_div.text.strip()
                    movie_name = full_title.replace(' - IMDb', '').split('(')[0].strip()
                else:
                    movie_name = "Unknown Title"

                rating = Links.__extract_rating(soup)  

                rating_info.append([movie_id, movie_name, rating])

            except Exception as e:
                print(f"Error fetching data for movie ID {movie_id}: {e}")

        rating_info.sort(key=lambda x: x[2] if x[2] is not None else 0, reverse=True)
        return rating_info


class Movies:
    def __init__(self, path_to_file):
        self._path = path_to_file
        self.movies_list = self.__load_file(max_lines=1000)
        self.movies_dict = {m["movieId"]: m for m in self.movies_list}

    def __load_file(self, max_lines=1000):
        movies = []
        try:
            with open(self._path, 'r', encoding='utf-8') as file:
                header = file.readline().strip()
                if header != "movieId,title,genres":
                    raise ValueError("Invalid file structure. Expected header: 'movieId,title,genres'")
                for line_number, line in enumerate(file, start=1):
                    if max_lines is not None and line_number > max_lines:
                        break  # достигли лимит строк

                    line = line.strip()
                    if not line:
                        continue

                    # Отделяем жанры с конца строки
                    last_comma = line.rfind(',')
                    if last_comma == -1:
                        continue  # строка некорректна

                    genres_str = line[last_comma + 1:].strip()
                    left = line[:last_comma]

                    # Теперь отделим movieId и title
                    first_comma = left.find(',')
                    if first_comma == -1:
                        continue
                    movie_id_str = left[:first_comma].strip()

                    title = left[first_comma + 1:].strip()
                    if '"' in title:
                        if title.split('"')[0] != '' or title.split('"')[-1] != '':
                            continue
                    else:
                        if len(title.split(',')) != 1:
                            continue
                    title = title.strip('"')

                    genres = [g.strip() for g in genres_str.split('|')] if genres_str else []

                    try:
                        movies.append({
                            "movieId": int(movie_id_str),
                            "title": title,
                            "genres": genres
                        })
                    except ValueError:
                        continue

        except Exception as e:
            print(f"Ошибка при чтении файла Movies: {e}")
        return movies

    def get_movies(self):
        return self.movies_list
    
    def get_movie_id_by_title(self, title):
        for movie in self.movies_list:
            if movie.get("title", "").lower() == title.lower():
                return int(movie["movieId"])
        return None

    def dist_by_release(self):
        release_years = defaultdict(int)
        for movie in self.movies_list:
            title = movie.get("title")
            if not title or not isinstance(title, str):
                continue  # пропускаем, если нет заголовка или он не строка
        
            match = re.search(r"\((\d{4})\)", title)
            if match:
                year = match.group(1)
                release_years[year] += 1
        return OrderedDict(sorted(release_years.items(), key=lambda x: x[1], reverse=True))
    
    def dist_by_genres(self):
        genres = Counter()
        for movie in self.movies_list:
            for genre in movie["genres"]:  # уже список
                genres[genre] += 1
        return dict(genres.most_common())

    def most_genres(self, n):
        try:
            if n < 0:
                raise Exception("n >= 0")
            movies = {
                movie["title"]: len(movie["genres"])
                for movie in self.movies_list
            }
            return OrderedDict(sorted(movies.items(), key=lambda x: x[1], reverse=True)[:n])
        except Exception as e:
            print(f"error: {e}")
            return {}

    def movies_by_genre(self, genre):
        return [movie["title"] for movie in self.movies_list if genre in movie["genres"]]

    def movies_by_year(self, year):
 
        movies = [
            {"title": movie["title"], "genres": movie["genres"]} 
            for movie in self.movies_list 
            if f"({year})" in movie["title"]
        ]
        
        if not movies:
            return []

        def dist_by_genres(movies_subset):
            genres = Counter()
            for movie in movies_subset:
                for genre in movie["genres"]:
                    genres[genre] += 1
            return dict(genres.most_common())

        genre_distribution = dist_by_genres(movies)
        most_common_genre = next(iter(genre_distribution.items())) if genre_distribution.items() else (None,0)

        def movie_popularity(genres):
            return sum(genre_distribution.get(g, 0) for g in genres)

        sorted_movies = sorted(
            movies,
            key=lambda x: movie_popularity(x["genres"]),
            reverse=True
        )
        print(f'total movies: {len(movies)}, most popularity genre: {most_common_genre}')

        return [{m["title"]: m["genres"]} for m in sorted_movies]

    def common_genre_combinations(self, n=10):
        try:
            if n < 0:
                raise Exception('n > 0')
            combo_counts = Counter(
            '|'.join(sorted(movie["genres"]))
            for movie in self.movies_list if movie["genres"]
            )
            return dict(combo_counts.most_common(n))
        except Exception as e:
            print(f"error: {e}")
            return {}
    
class Ratings:
    def __init__(self, path_to_the_file, movies_file, movie_ids):
        self._path = path_to_the_file
        self._movies_path = movies_file
        self.ratings = []
        self.movie_titles = {}
        self.movies = []

        # Загружаем рейтинги
        ratings_data = read_csv_as_dict(self._path, count_lines=1000, valid_movie_ids=movie_ids)
        for row in ratings_data:
            try:
                self.ratings.append({
                    "userId": int(row.get("userId", 0)),
                    "movieId": int(row.get("movieId", 0)),
                    "rating": float(row.get("rating", 0.0)),
                    "timestamp": int(row.get("timestamp", 0))
                })
            except Exception as e:
                print(f"Ошибка при чтении файла: {e}")

        movies_data = self.__load_file(max_lines=1000)
        for row in movies_data:
            try:
                movie_id = int(row.get("movieId", 0))
            except ValueError:
                continue
            if not movie_id:
                continue
            row["movieId"] = movie_id
            row["title"] = row.get("title", "")
            row["genres"] = row.get("genres", [])
            self.movie_titles[movie_id] = row["title"]
            self.movies.append(row)

    def __load_file(self, max_lines=1000):
        movies = []
        try:
            with open(self._movies_path, 'r', encoding='utf-8') as file:
                header = file.readline().strip()
                if header != "movieId,title,genres":
                    raise ValueError("Invalid file structure. Expected header: 'movieId,title,genres'")
                for line_number, line in enumerate(file, start=1):
                    if max_lines is not None and line_number > max_lines:
                        break  # достигли лимит строк

                    line = line.strip()
                    if not line:
                        continue

                    # Отделяем жанры с конца строки
                    last_comma = line.rfind(',')
                    if last_comma == -1:
                        continue  # строка некорректна

                    genres_str = line[last_comma + 1:].strip()
                    left = line[:last_comma]

                    # Теперь отделим movieId и title
                    first_comma = left.find(',')
                    if first_comma == -1:
                        continue
                    movie_id_str = left[:first_comma].strip()

                    title = left[first_comma + 1:].strip()
                    if '"' in title:
                        if title.split('"')[0] != '' or title.split('"')[-1] != '':
                            continue
                    else:
                        if len(title.split(',')) != 1:
                            continue
                    title = title.strip('"')

                    genres = [g.strip() for g in genres_str.split('|')] if genres_str else []

                    try:
                        movies.append({
                            "movieId": int(movie_id_str),
                            "title": title,
                            "genres": genres
                        })
                    except ValueError:
                        continue

        except Exception as e:
            print(f"Ошибка при чтении файла Movies: {e}")
        return movies

    def get_ratings_for_movies(self, movie_ids):
        return [r["rating"] for r in self.ratings if r["movieId"] in movie_ids]

    @staticmethod
    def extract_year_from_title(title: str) -> int | None:
        if not title or not isinstance(title, str):
            return None
        start = title.rfind("(")
        end = title.rfind(")")
        if start != -1 and end != -1 and end > start:
            year_str = title[start + 1:end]
            if year_str.isdigit() and len(year_str) == 4:
                return int(year_str)
        return None

    class Movies:
        def __init__(self, parent, movies_list):
            self.parent = parent
            self.ratings = parent.ratings
            self.movie_titles = parent.movie_titles
            self.movies = movies_list

        def dist_by_year(self):
            result = defaultdict(int)
            for r in self.ratings:
                try:
                    timestamp = int(r["timestamp"])
                    year = datetime.fromtimestamp(timestamp, tz=timezone.utc).year
                    result[year] += 1
                except (KeyError, ValueError, TypeError) as e:
                    print(f"⚠️ Пропущена запись с ошибкой: {r} — {e}")
                    continue
            return dict(sorted(result.items()))

        def dist_by_rating(self):
            result = defaultdict(int)
            for r in self.ratings:
                try:
                    result[r["rating"]] += 1
                except (KeyError, ValueError, TypeError) as e:
                    print(f"⚠️ Пропущена запись с ошибкой: {r} — {e}")
                    continue
            return dict(sorted(result.items()))

        def top_by_num_of_ratings(self, n):
            counts = defaultdict(int)
            for r in self.ratings:
                try:
                    counts[r["movieId"]] += 1
                except (KeyError, ValueError, TypeError) as e:
                    print(f"⚠️ Пропущена запись с ошибкой: {r} — {e}")
                    continue
            sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]
            return {self.movie_titles[mid]: count for mid, count in sorted_counts}

        def top_by_ratings(self, n, metric="average"):
            scores = defaultdict(list)
            for r in self.ratings:
                scores[r["movieId"]].append(r["rating"])
            result = {}
            for mid, values in scores.items():
                if len(values) < 2:
                    continue
                result[mid] = round(mean(values), 2) if metric == "average" else round(median(values), 2)
            top = sorted(result.items(), key=lambda x: x[1], reverse=True)[:n]
            return {
                self.movie_titles[mid]: score
                for mid, score in top
                if mid in self.movie_titles 
            }

        def top_controversial(self, n):
            scores = defaultdict(list)
            for r in self.ratings:
                scores[r["movieId"]].append(r["rating"])
            variances = {}
            for mid, vals in scores.items():
                if len(vals) < 2:
                    continue
                variances[mid] = round(variance(vals), 2)
            top = sorted(variances.items(), key=lambda x: x[1], reverse=True)[:n]
            return {self.movie_titles[mid]: var
                    for mid, var in top
                    if mid in self.movie_titles}
        
        def average_genre_rating_by_year(self, genre_filter=None, release_year=None):
            movie_genres = {}
            movie_years = {}
            matching_movies = set()
            for movie in self.movies:
                movie_id = movie.get("movieId")
                title = movie.get("title", "")
                genres = movie.get("genres", "")
                year = Ratings.extract_year_from_title(title)
                if year is not None:
                    movie_years[movie_id] = year
                if isinstance(genres, str):
                    genres = genres.split("|")
                movie_genres[movie_id] = genres
                genre_match = genre_filter is None or genre_filter in genres
                year_match = release_year is None or year == release_year
                if genre_match and year_match:
                    matching_movies.add(movie_id)
            rating_by_year = defaultdict(list)
            for r in self.ratings:
                movie_id = r["movieId"]
                if movie_id not in matching_movies:
                    continue
                try:
                    ts = int(r["timestamp"])
                    rating_year = datetime.fromtimestamp(ts).year
                    rating = float(r["rating"])
                except (ValueError, KeyError, TypeError):
                    continue
                rating_by_year[rating_year].append(rating)
            result = {
                year: {
                    "count": len(ratings),
                    "average_rating": round(mean(ratings), 2)
                }
                for year, ratings in sorted(rating_by_year.items())
            }
            return result
                

    class Users:
        def __init__(self, parent, movies):
            self.parent = parent
            self.ratings = parent.ratings
            self.movie_genres = {}
            self.movie_years = {}

            for m in movies:
                title = m.get("title", "")
                movie_id = m.get("movieId")
                if not isinstance(title, str) or movie_id is None:
                    continue

                self.movie_genres[movie_id] = m.get("genres", [])

                year = Ratings.extract_year_from_title(title)
                if year is not None:
                    self.movie_years[movie_id] = year

        def dist_by_num_of_ratings(self):
            rating_to_users = defaultdict(set)
            for r in self.ratings:
                try:
                    rating = r["rating"]
                    user_id = r["userId"]
                    rating_to_users[rating].add(user_id)
                except (KeyError, ValueError, TypeError) as e:
                    print(f"{r} — {e}")
                    continue
            result = {rating: len(users) for rating, users in rating_to_users.items()}
            return dict(sorted(result.items()))
                    
        def dist_by_user_rating(self, metric="average"):
            users = defaultdict(list)
            for r in self.ratings:
                try:
                    users[r["userId"]].append(r["rating"])
                except (KeyError, TypeError):
                    continue
            dist = defaultdict(int)
            for ratings in users.values():
                if not ratings:
                    continue
                val = round(mean(ratings), 1) if metric == "average" else round(median(ratings), 1)
                dist[val] += 1
            return dict(sorted(dist.items()))

        def top_controversial(self, n):
            users = defaultdict(list)
            for r in self.ratings:
                users[r["userId"]].append(r["rating"])
            variances = {}
            for uid, vals in users.items():
                if len(vals) < 2:
                    continue
                variances[uid] = round(variance(vals), 2)
            top = sorted(variances.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)

        def genre_rating_trend_by_year(self, genre_filter: str = "Drama"):
            ratings_by_year = defaultdict(list)
            users_by_year = defaultdict(set)
            for r in self.ratings:
                movie_id = r["movieId"]
                genres = self.movie_genres.get(movie_id, [])
                if genre_filter in genres:
                    rating_year = datetime.fromtimestamp(r["timestamp"]).year
                    ratings_by_year[rating_year].append(r["rating"])
                    users_by_year[rating_year].add(r["userId"])
            result = {
                year: {
                    "Средний рейтинг": round(mean(ratings), 2), 
                    "оценок": len(ratings),
                    "пользователей": len(users_by_year[year]) 
                }
                for year, ratings in sorted(ratings_by_year.items())
            }
            return result


class Tags:
    def __init__(self, path_to_the_file, movie_ids):
        self.tags = set()
        self.tag_list = []
        self.movie_tags = {}
        self.valid_movie_ids = set(movie_ids)

        rows = read_csv_as_dict(path_to_the_file, count_lines=1000, valid_movie_ids=movie_ids)
        for row in rows:
            try:
                tag = row.get("tag", "").strip()
                movie_id = int(row.get("movieId", 0))

                if tag:
                    self.tags.add(tag)
                    self.tag_list.append(tag)
                    self.movie_tags.setdefault(movie_id, []).append(tag)
            except Exception as e:
                print(f"Ошибка при обработке строки: {row}, ошибка: {e}")

    def most_words(self, n):
        return dict(
            sorted(
                {tag: len(tag.split()) for tag in self.tags}.items(),
                key=lambda x: (-x[1], x[0]),  # Сортировка по убыванию слов, затем по алфавиту
            )[:n]
        )

    def longest(self, n):
        return sorted(self.tags, key=lambda x: (-len(x), x))[:n]

    def most_words_and_longest(self, n):
        top_words = set(self.most_words(n).keys())
        top_longest = set(self.longest(n))
        intersected = top_words & top_longest
        return sorted(intersected, key=lambda tag: (-len(tag.split()), -len(tag), tag))

    def most_popular(self, n):
        return dict(Counter(self.tag_list).most_common(n))

    def tags_with(self, word):
        word = word.lower()
        return sorted({tag for tag in self.tags if word in tag.lower()})

    def get_all_tags(self):
        return self.movie_tags

    def top_movies_by_tag(self, tag_name, ratings_obj, movies_obj, n=10):
        tag_name = tag_name.lower()

        movies = movies_obj.get_movies()
        movie_title_map = {int(m['movieId']): m['title'] for m in movies}

        movie_ids = [
            mid for mid, tags in self.movie_tags.items()
            if mid in self.valid_movie_ids and any(tag_name in t.lower() for t in tags)
        ]
        avg_ratings = {}
        for mid in movie_ids:
            ratings = ratings_obj.get_ratings_for_movies([mid])
            if ratings:
                avg = sum(ratings) / len(ratings)
                title = movie_title_map.get(mid, f"[ID {mid}]")
                avg_ratings[title] = round(avg, 2)

        sorted_avg = dict(sorted(avg_ratings.items(), key=lambda x: x[1], reverse=True)[:n])
        return sorted_avg

    def tag_statistics(self, movies_obj):
        movies = movies_obj.get_movies()

        valid_movie_ids = set(int(m.get("movieId")) for m in movies)

        movie_title_map = {int(m['movieId']): m['title'] for m in movies}
        
        stats = {}
        for mid, tags in self.movie_tags.items():

            movie_id = int(mid)
            if movie_id in valid_movie_ids:
                title = movie_title_map.get(movie_id, f"[ID {movie_id}]")
                stats[title] = len(tags)

        sorted_stats = dict(sorted(stats.items(), key=lambda x: x[1], reverse=True))

        return sorted_stats
    
    def get_tags_for_movie(self, title, movies_obj):
        movie_id = movies_obj.get_movie_id_by_title(title)
        if movie_id is None:
            print(f"Фильм '{title}' не найден.")
            return []
        return self.movie_tags.get(movie_id, [])
    


class Tests:

    TEST_MOVIES_FILE = "test_movies.csv"
    TEST_RATINGS_FILE = "test_raitings.csv"
    TEST_LINKS_FILE = "test_links.csv"
    TEST_TAGS_FILE = "test_tags.csv"
    MOVIES_FILE = '../datasets/ml-latest-small/movies.csv'
    RATINGS_FILE = "../datasets/ml-latest-small/ratings.csv"
    LINKS_FILE = "../datasets/ml-latest-small/links.csv"
    TAGS_FILE = "../datasets/ml-latest-small/tags.csv"


    class TestHelpers:
        
        def test_read_file(self, count_lines=10):
            gen = read_file(Tests.MOVIES_FILE, count_lines=count_lines)
            lines = list(gen)
            assert len(lines) == count_lines
            assert 'generator' in str(type(gen))
            assert len(lines) == count_lines
            assert all(isinstance(line, str) for line in lines)

        def test_read_csv_as_dict(self):
            data = read_csv_as_dict(Tests.TEST_MOVIES_FILE)
            assert isinstance(data, list)
            assert all(isinstance(row, dict) for row in data)

        def test_mean(self):
            assert isinstance(mean([1, 2, 3, 4, 5]), float)
            assert mean([1, 2, 3, 4, 5]) == 3.0
            assert mean([]) == 0

        def test_median(self):
            assert isinstance(mean([1, 2, 3, 4, 5]), float)
            assert median([1, 3, 2]) == 2
            assert median([1, 3, 2, 4]) == 2.5
            assert median([]) == 0

        def test_variance(self):
            assert isinstance(variance([1, 2, 3, 4, 5]), float)
            assert variance([1, 2, 3, 4, 5]) == pytest.approx(2.5)
            assert variance([]) == 0

    class TestLinksClass:

        @pytest.fixture(scope="module")
        def links_obj(self):
            return Links(Tests.LINKS_FILE)
       
        def test_links_init(self, links_obj):
            assert isinstance(links_obj._path_to_the_file, str)
            assert isinstance(links_obj.dict_file, list)
            assert isinstance(links_obj.imdb_info, list)
        #     print(Tests.links.imdb_info == Tests.links._Links__imdb_getter())
    
        def test_read_csv_column(self, links_obj):
            movies = Movies(Tests.MOVIES_FILE)
            movies_list = movies.get_movies()
            movie_ids = set(int(m["movieId"]) for m in movies_list)
            result = links_obj.read_csv_column(file_path=Tests.LINKS_FILE, column_name="imdbId", valid_movie_ids=movie_ids)[:10]
            assert isinstance(result, list)
            assert all(isinstance(item, str) for item in result)
            assert result == ['0114709','0113497','0113228','0114885','0113041','0113277','0114319','0112302','0114576','0113189']

        def test_get_imdb(self, links_obj):
            result = links_obj.get_imdb(list_of_movies=['0114709', '0113497', '0113228'], list_of_fields=['Director', 'Budget', 'Gross worldwide', 'Runtime', 'Title'])
            ans = [['0114709','John Lasseter','$30,000,000 (estimated)','$394,436,586','1 hour 21 minutes', 'История игрушек'],
                   ['0113497', 'Joe Johnston', '$65,000,000 (estimated)', '$262,821,940', '1 hour 44 minutes', 'Джуманджи'],
                   ['0113228', 'Howard Deutch', '$25,000,000 (estimated)', '$71,518,503', '1 hour 41 minutes','Старые ворчуны разбушевались']
                  ]
            assert isinstance(result, list)
            assert all(isinstance(i, list) for i in result)
            assert all(isinstance(i, str) for j in result for i in j)
            assert result == sorted(result, key=lambda x: x[0], reverse=True)
            assert result == ans

        def test_top_directors(self, links_obj):
            result = links_obj.top_directors(3)
            ans = {'Forest Whitaker': 1, 'John Lasseter': 1, 'Peter Hyams': 1}
            assert isinstance(result, dict)
            assert all(isinstance(i, str) for i in result.keys())
            assert all(isinstance(i, int) for i in result.values())
            assert result == dict(sorted(result.items(), key=lambda x: x[1], reverse=True))
            assert result == ans

        def test_most_expensive(self, links_obj):
            result = links_obj.most_expensive(3)
            ans = {'Джуманджи': 65000000.0, 'Схватка': 60000000.0, 'Золотой глаз': 60000000.0}
            assert isinstance(result, dict)
            assert all(isinstance(i, str) for i in result.keys())
            assert all(isinstance(i, float) for i in result.values())
            assert result == dict(sorted(result.items(), key=lambda x: x[1], reverse=True))
            assert result == ans

        def test_most_profitable(self, links_obj):
            result = links_obj.most_profitable(3)
            ans = {'История игрушек': 364436586.0, 'Золотой глаз': 292194034.0, 'Джуманджи': 197821940.0}
            assert isinstance(result, dict)
            assert all(isinstance(title, str) for title in result.keys())
            assert all(isinstance(profit, float) for profit in result.values())
            assert result == dict(sorted(result.items(), key=lambda x: x[1], reverse=True))
            assert result == ans

        def test_longest(self, links_obj):
            result = links_obj.longest(3)
            ans = {'Схватка': 170, 'Золотой глаз': 130, 'Сабрина': 127}
            assert isinstance(result, dict)
            assert all(isinstance(title, str) for title in result.keys())
            assert all(isinstance(runtime, int) for runtime in result.values())
            assert result == dict(sorted(result.items(), key=lambda x: x[1], reverse=True))
            assert result == ans

        def test_top_cost_per_minute(self, links_obj):
            result = links_obj.top_cost_per_minute(3)
            ans = {'Джуманджи': 625000.0, 'Золотой глаз': 461538.46153846156, 'Сабрина': 456692.91338582675}
            assert isinstance(result, dict)
            assert all(isinstance(title, str) for title in result.keys())
            assert all(isinstance(cost, float) for cost in result.values())
            assert result == ans

        def test_get_imdb_rating(self, links_obj):
            result = links_obj.get_imdb_rating(['0114709', '0113277'])
            ans = [['0114709', 'История игрушек', '8.3/10'], ['0113277', 'Схватка', '8.3/10']]
            assert isinstance(result, list)
            assert all(isinstance(item, list) for item in result)
            assert all(len(item) == 3 for item in result)  # movieId, name, rating
            ratings = [float(item[2].split('/')[0]) if item[2] else 0 for item in result]
            assert all(ratings[i] >= ratings[i+1] for i in range(len(ratings)-1))
            assert result == ans

    class TestMoviesClass:
        """Тесты для класса Movies"""
        
        @pytest.fixture(scope="module")
        def movies_obj(self):
            """Фикстура для создания объекта Movies"""
            return Movies(Tests.MOVIES_FILE)

        def test_movies_init(self, movies_obj):
            """Тестирование инициализации класса Movies"""
            assert isinstance(movies_obj.movies_list, list)
            assert all(isinstance(movie, dict) for movie in movies_obj.movies_list)
            assert "movieId" in movies_obj.movies_list[0]
            assert "title" in movies_obj.movies_list[0]
            assert "genres" in movies_obj.movies_list[0]


        def test_load_file(self, movies_obj):
            result = movies_obj._Movies__load_file()
            assert isinstance(result, list)
            if result == []:
                with pytest.raises(Exception) as e:
                    result
                assert str(e.value) == f"Ошибка при чтении файла Movies: {e}"
            else:
                assert all(isinstance(i, dict) for i in result)
                for movie in result:
                    assert isinstance(movie["movieId"], int)
                    assert isinstance(movie["title"], str)
                    assert isinstance(movie["genres"], list)
                    assert all(isinstance(i, str) for i in movie["genres"])

        def test_get_movies(self, movies_obj):
            result = movies_obj.get_movies()
            assert isinstance(result, list)
            if result == []:
                with pytest.raises(Exception) as e:
                    result
                assert str(e.value) == f"Ошибка при чтении файла Movies: {e}"
            else:
                assert all(isinstance(i, dict) for i in result)
                for movie in result:
                    assert isinstance(movie["movieId"], int)
                    assert isinstance(movie["title"], str)
                    assert isinstance(movie["genres"], list)
                    assert all(isinstance(i, str) for i in movie["genres"])
            

        def test_dist_by_release(self, movies_obj):
            result = movies_obj.dist_by_release()
            assert isinstance(result, OrderedDict)
            assert all([isinstance(i, str) for i in result.keys()])
            assert all([isinstance(i, int) for i in result.values()])
            counts = list(result.values())
            assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
            assert result['1995'] == 224
            assert result['1994'] == 184
            assert result['1996'] == 181
            assert result['1993'] == 101

        def test_dist_by_genres(self, movies_obj):
            result = movies_obj.dist_by_genres()
            assert isinstance(result, dict)
            assert all([isinstance(i, str) for i in result])
            assert all([isinstance(i, int) for i in result.values()])
            counts = list(result.values())
            assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
            assert result['Thriller'] == 179
            assert result['Action'] == 158
            assert result['Mystery'] == 58

        
        @pytest.mark.parametrize("n, answer", [
            (3, OrderedDict({'Strange Days (1995)': 6, 'Lion King, The (1994)': 6, 'Getaway, The (1994)': 6})),
            (5, OrderedDict({
                'Strange Days (1995)': 6,
                'Lion King, The (1994)': 6,
                'Getaway, The (1994)': 6,
                'Super Mario Bros. (1993)': 6,
                'Beauty and the Beast (1991)': 6})),
            (-2, {})
        ])
        
        def test_most_genres(self, movies_obj, n, answer):
            result = movies_obj.most_genres(n)
            if result != {}:
                assert isinstance(result, OrderedDict)
                assert all(isinstance(i, str) for i in result.keys())
                assert all(isinstance(i, int) for i in result.values())
                counts = list(result.values())
                assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
            assert result == answer


        @pytest.mark.parametrize("genre", 
                [
                    ("Adventure|Animation|Children|Comedy|Fantasy"),
                    ("Children|Comedy|Fantasy"),
                    ("Fantasy"),
                ]
        )
        def test_movies_by_genre(self, movies_obj, genre):
            result = movies_obj.movies_by_genre(genre)
            assert isinstance(result, list)
            assert all(isinstance(i, str) for i in result)
            

        @pytest.mark.parametrize("year, answer", 
                [
                    (1995, [{'Money Train (1995)': ['Action', 'Comedy', 'Crime', 'Drama', 'Thriller']},
                            {'Bad Boys (1995)': ['Action', 'Comedy', 'Crime', 'Drama', 'Thriller']}]),
                    (1996, [{'Fargo (1996)': ['Comedy', 'Crime', 'Drama', 'Thriller']},
                            {'Freeway (1996)': ['Comedy', 'Crime', 'Drama', 'Thriller']}]),
                    (1997,[{"'Til There Was You (1997)": ['Drama', 'Romance']},
                           {'Bliss (1997)': ['Drama', 'Romance']}])
                ]
        )

        def test_movies_by_year(self, movies_obj, year, answer):

            result = movies_obj.movies_by_year(year)
            assert isinstance(result, list)

            if result:
                assert all(isinstance(i, dict) for i in result)
                for movie in result:
                    assert all(isinstance(i, str) for i in movie.keys())
                    val = movie.values()
                    assert all(isinstance(i, list) for i in val)
                    assert all(isinstance(i, str) for j in val for i in j)
                assert result[0] == answer[0]
                assert result[1] == answer[1]

        @pytest.mark.parametrize("n, answer", 
                [
                    (10, {'Drama': 142, 'Comedy': 94,'Comedy|Drama': 54, 'Drama|Romance': 51,
                          'Comedy|Romance': 43, 'Comedy|Drama|Romance': 28, 'Documentary': 23,
                          'Drama|Thriller': 20, 'Crime|Drama': 19, 'Drama|War': 17}),
                    (1,{'Drama': 142}),
                    (-2, {}),
                ]
        )

        def test_common_genre_combinations(self, movies_obj, n, answer):
            result = movies_obj.common_genre_combinations(n)
            assert isinstance(result, dict)
            assert all(isinstance(i, str) for i in result.keys())
            assert all(isinstance(i, int) for i in result.values())
            counts = list(result.values())
            assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
            assert result == answer
        

    class TestRatingsClass:
        @pytest.fixture
        def ratings_obj(self):
            movies = Movies(Tests.MOVIES_FILE)
            movies_list = movies.get_movies()
            movie_ids = set(int(m["movieId"]) for m in movies_list)
            ratings_obj = Ratings(Tests.RATINGS_FILE, Tests.MOVIES_FILE, movie_ids)
            return ratings_obj

        def test_ratings_init(self, ratings_obj):
            file_path = ratings_obj._path
            movies_path = ratings_obj._movies_path
            assert isinstance(file_path, str)
            assert isinstance(movies_path, str)


        def test_ratings_init_raitings(self, ratings_obj):

            ratings = ratings_obj.ratings

            assert isinstance(ratings, list)
            assert all(isinstance(rating, dict) for rating in ratings)
            # assert "userId" in ratings[0]
            # assert "movieId" in ratings[0]
            # assert "rating" in ratings[0]
            # assert "timestamp" in ratings[0]

            for rating in ratings:
                assert isinstance(rating["userId"], int)
                assert isinstance(rating["movieId"], int)
                assert isinstance(rating["rating"], float)
                assert isinstance(rating["timestamp"], int)

        def test_ratings_init_movie_titles(self, ratings_obj):

            assert isinstance(ratings_obj.movie_titles, dict)
            assert all(isinstance(i, int) for i in ratings_obj.movie_titles.keys())
            assert all(isinstance(i, str) for i in ratings_obj.movie_titles.values())


        def test_ratings_init_movies(self, ratings_obj):

            assert isinstance(ratings_obj.movies, list)
            assert all(isinstance(movie, dict) for movie in ratings_obj.movies)

            for movie in ratings_obj.movies:
                assert isinstance(movie["movieId"], int)
                assert isinstance(movie["title"], str)
                assert isinstance(movie["genres"], list)
                assert all(isinstance(i, str) for i in movie["genres"])

        def test_load_file(self, ratings_obj):
            result = ratings_obj._Ratings__load_file(max_lines=10)
            print(result)
            assert isinstance(result, list)
            for movie in result:
                    assert isinstance(movie["movieId"], int)
                    assert isinstance(movie["title"], str)
                    assert isinstance(movie["genres"], list)
                    assert all(isinstance(i, str) for i in movie["genres"])

        def test_get_ratings_for_movies(self, ratings_obj):
            result = ratings_obj.get_ratings_for_movies(ratings_obj.movies)
            print(result)
            assert isinstance(result, list)

        def test_extract_year_from_title(self):
            """Тестирование извлечения года из названия"""
            assert type(Ratings.extract_year_from_title("Toy Story (1995)")) is int
            assert Ratings.extract_year_from_title("Toy Story (1995)") == 1995
            assert Ratings.extract_year_from_title("No year here") is None
            assert Ratings.extract_year_from_title("") is None

        class TestRatingsMoviesSubclass:
            
            @pytest.fixture
            def ratings_movies_obj(self, ratings_obj):
                return ratings_obj.Movies(ratings_obj, ratings_obj.movies)

            def test_dist_by_year(self, ratings_movies_obj):

                result = ratings_movies_obj.dist_by_year()
                answer = {1996: 453, 1998: 36, 1999: 47, 2000: 127, 2001: 21, 2003: 11,
                          2005: 31, 2009: 12, 2011: 67, 2012: 2, 2013: 52, 2015: 2,
                          2016: 111, 2017: 26, 2018: 2}
                assert isinstance(result, dict)
                assert all(isinstance(year, int) for year in result.keys())
                assert all(isinstance(count, int) for count in result.values())
                years = list(result.keys())
                assert all(years[i] <= years[i+1] for i in range(len(years)-1))
                assert result == answer


            def test_dist_by_rating(self, ratings_movies_obj):
                result = ratings_movies_obj.dist_by_rating()
                answer = {0.5: 11, 1.0: 26, 1.5: 2, 2.0: 47, 2.5: 13, 3.0: 287,
                          3.5: 41, 4.0: 303, 4.5: 59, 5.0: 211}
                assert isinstance(result, dict)
                assert all(isinstance(rating, float) for rating in result.keys())
                assert all(isinstance(count, int) for count in result.values())
                ratings = list(result.keys())
                assert all(ratings[i] <= ratings[i+1] for i in range(len(ratings)-1))
                assert result == answer

            def test_top_by_num_of_ratings(self, ratings_movies_obj):
                result = ratings_movies_obj.top_by_num_of_ratings(3)
                answer = {'Pulp Fiction (1994)': 11, 'Forrest Gump (1994)': 11, 'Seven (a.k.a. Se7en) (1995)': 10}
                assert isinstance(result, dict)
                assert all(isinstance(title, str) for title in result.keys())
                assert all(isinstance(count, int) for count in result.values())
                counts = list(result.values())
                assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
                assert result == answer


            @pytest.mark.parametrize("metric, answer", 
                [
                    ("average", {'Ghost and the Darkness, The (1996)': 5.0, 'Fantasia (1940)': 5.0,
                                 'First Knight (1995)': 5.0}), 
                    ("median", {'Star Wars: Episode IV - A New Hope (1977)': 5.0, 'Tommy Boy (1995)': 5.0,
                                'Fugitive, The (1993)': 5.0})
                ]
            )
            def test_top_by_ratings(self, ratings_movies_obj, metric, answer):
                result = ratings_movies_obj.top_by_ratings(3, metric=metric)
                assert isinstance(result, dict)
                assert all(isinstance(title, str) for title in result.keys())
                assert all(isinstance(rating, float) for rating in result.values())
                ratings = list(result.values())
                assert all(ratings[i] >= ratings[i+1] for i in range(len(ratings)-1))
                assert result == answer 
            
            def test_top_controversial(self, ratings_movies_obj):
                result = ratings_movies_obj.top_controversial(3)
                answer = {'My Fair Lady (1964)': 10.12,
                          "City Slickers II: The Legend of Curly's Gold (1994)": 8.0,
                          'Courage Under Fire (1996)': 6.12}
                assert isinstance(result, dict)
                assert all(isinstance(title, str) for title in result.keys())
                assert all(isinstance(variance, float) for variance in result.values())
                # Проверка сортировки по убыванию дисперсии
                variances = list(result.values())
                assert all(variances[i] >= variances[i+1] for i in range(len(variances)-1))
                assert result == answer



            @pytest.mark.parametrize("genre, year, answer", 
                [
                    ('Comedy', 1996, {1996: {'count': 30, 'average_rating': 3.03}, 2000: {'count': 11, 'average_rating': 4.09},
                                     2001: {'count': 2, 'average_rating': 3.0}, 2009: {'count': 2, 'average_rating': 4.5},
                                     2011: {'count': 1, 'average_rating': 0.5}, 2013: {'count': 1, 'average_rating': 4.5},
                                     2016: {'count': 6, 'average_rating': 3.33}}
                    ), 
                    ('Drama', 1992, {1996: {'count': 3, 'average_rating': 3.0},
                                      2000: {'count': 2, 'average_rating': 4.0},
                                      2001: {'count': 1, 'average_rating': 4.0},
                                      2003: {'count': 1, 'average_rating': 4.0},
                                      2017: {'count': 1, 'average_rating': 4.0}}
                    )                  
                ]       
            )
            def test_average_genre_rating_by_year(self, ratings_movies_obj, genre, year, answer):
                result = ratings_movies_obj.average_genre_rating_by_year(genre_filter=genre, release_year=year)
                assert isinstance(result, dict)
                for year, data in result.items():
                    assert isinstance(year, int)
                    assert isinstance(data, dict)
                    assert "count" in data
                    assert "average_rating" in data
                    assert isinstance(data["count"], int)
                    assert isinstance(data["average_rating"], float)
                assert result == answer


        class TestRatingsUsersSubclass:
            """Тесты для вложенного класса Users в Ratings"""
            
            @pytest.fixture
            def ratings_users_obj(self, ratings_obj):
                return ratings_obj.Users(ratings_obj, ratings_obj.movies)

            def test_dist_by_num_of_ratings(self, ratings_users_obj):
                """Тестирование распределения по количеству оценок"""
                result = ratings_users_obj.dist_by_num_of_ratings()
                answer = {0.5: 2, 1.0: 8, 1.5: 1, 2.0: 13, 2.5: 4, 3.0: 17,
                        3.5: 8, 4.0: 17, 4.5: 5, 5.0: 16}
                assert isinstance(result, dict)
                assert all(isinstance(num_ratings, float) for num_ratings in result.keys())
                assert all(isinstance(count, int) for count in result.values())
                # Проверка сортировки по возрастанию количества оценок
                nums = list(result.keys())
                assert all(nums[i] <= nums[i+1] for i in range(len(nums)-1))
                assert result == answer

            @pytest.mark.parametrize("metric, answer", 
                [
                 ("average",{1.1: 1, 2.8: 2, 3.4: 2, 3.5: 3, 3.6: 2, 3.7: 2, 3.8: 2,
                             3.9: 2, 4.2: 1, 4.3: 1, 4.4: 1}),
                 ("median",{0.5: 1, 3.0: 5, 3.2: 1, 3.5: 1, 3.8: 1, 4.0: 7, 4.2: 1, 4.5: 1, 5.0: 1})
                ]
            )
            def test_dist_by_user_rating(self, ratings_users_obj, metric, answer):
                result = ratings_users_obj.dist_by_user_rating(metric=metric)
                assert isinstance(result, dict)
                assert all(isinstance(rating, float) for rating in result.keys())
                assert all(isinstance(count, int) for count in result.values())
                # Проверка сортировки по возрастанию рейтинга
                ratings = list(result.keys())
                assert all(ratings[i] <= ratings[i+1] for i in range(len(ratings)-1))
                assert result == answer

            def test_top_controversial_users(self, ratings_users_obj):
                """Тестирование самых противоречивых пользователей"""
                result = ratings_users_obj.top_controversial(3)
                answer = {13: 2.8, 3: 2.23, 15: 2.04}
                assert isinstance(result, dict)
                assert all(isinstance(user_id, int) for user_id in result.keys())
                assert all(isinstance(variance, float) for variance in result.values())
                # Проверка сортировки по убыванию дисперсии
                variances = list(result.values())
                assert all(variances[i] >= variances[i+1] for i in range(len(variances)-1))
                assert result == answer


            def test_genre_rating_trend_by_year(self, ratings_users_obj):
                """Тестирование трендов рейтинга по жанрам"""
                result1 = ratings_users_obj.genre_rating_trend_by_year("Thriller")
                result2 = ratings_users_obj.genre_rating_trend_by_year("Drama")
                assert isinstance(result1, dict)
                for year, data in result1.items():
                    assert isinstance(year, int)
                    assert isinstance(data, dict)
                    assert "Средний рейтинг" in data
                    assert "оценок" in data
                    assert "пользователей" in data
                    assert isinstance(data["Средний рейтинг"], float)
                    assert isinstance(data["оценок"], int)
                    assert isinstance(data["пользователей"], int)
                assert result1[1998] == {'Средний рейтинг': 3.84, 'оценок': 19, 'пользователей': 1}
                assert result1[2001] ==  {'Средний рейтинг': 3.67, 'оценок': 3, 'пользователей': 2}
                assert result2[2000] == {'Средний рейтинг': 4.12, 'оценок': 41, 'пользователей': 2}
                assert result2[1998] == {'Средний рейтинг': 4.27, 'оценок': 11, 'пользователей': 1}


    class TestTagsClass:
        """Тесты для класса Tags"""
        
        @pytest.fixture
        def tags_obj(self):
            movies = Movies(Tests.MOVIES_FILE)
            movies_list = movies.get_movies()
            movie_ids = set(int(m["movieId"]) for m in movies_list)
            return Tags(Tests.TAGS_FILE, movie_ids)

        def test_tags_init(self, tags_obj):
            """Тестирование инициализации класса Tags"""
            assert isinstance(tags_obj.tags, set)
            assert all(isinstance(tag, str) for tag in tags_obj.tags)
            assert isinstance(tags_obj.tag_list, list)
            assert isinstance(tags_obj.movie_tags, dict)

        def test_most_words(self, tags_obj):
            result = tags_obj.most_words(10)
            assert isinstance(result, dict)
            assert all(isinstance(tag, str) for tag in result.keys())
            assert all(isinstance(count, int) for count in result.values())
            # Проверка сортировки по убыванию количества слов
            counts = list(result.values())
            assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
            assert result['villain nonexistent or not needed for good story'] == 8
            assert result['It was melodramatic and kind of dumb'] == 7

        def test_longest_tags(self, tags_obj):
            result = tags_obj.longest(6)
            answer = ['villain nonexistent or not needed for good story',
                        'r:disturbing violent content including rape',
                        'Oscar (Best Effects - Visual Effects)',
                        'It was melodramatic and kind of dumb',
                        'r:sustained strong stylized violence',
                        'Oscar (Best Music - Original Score)']
            assert isinstance(result, list)
            assert all(isinstance(tag, str) for tag in result)
            # Проверка сортировки по убыванию длины
            lengths = [len(tag) for tag in result]
            assert all(lengths[i] >= lengths[i+1] for i in range(len(lengths)-1))
            assert result == answer
        

        def test_tag_statistics(self, tags_obj):
            movie = Movies(Tests.MOVIES_FILE)
            result = tags_obj.tag_statistics(movie)
            assert isinstance(result, dict)
            assert all(isinstance(title, str) for title in result.keys())
            assert all(isinstance(count, int) for count in result.values())
            # Проверка сортировки по убыванию количества тегов
            counts = list(result.values())
            assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
            assert result['Pulp Fiction (1994)'] == 181
            assert result['Reservoir Dogs (1992)'] == 11
            assert result['Star Wars: Episode IV - A New Hope (1977)'] == 26
            
        def test_get_tags_for_movie(self, tags_obj):
            test_title = "Psycho (1960)"
            movie = Movies(Tests.MOVIES_FILE)
            result = tags_obj.get_tags_for_movie(test_title, movie)
            assert isinstance(result, list)
            assert all(isinstance(tag, str) for tag in result)
            assert tags_obj.get_tags_for_movie("Nonexistent Movie", movie) == []
            assert set(result) == {'Alfred Hitchcock', 'psychology', 'suspenseful', 'tense','Norman Bates',
                             'Alfred Hitchcock', 'black and white', 'imdb top 250','remade'}
            

        def test_most_words_and_longest(self, tags_obj):
            result = tags_obj.most_words_and_longest(3)
            answer = ['villain nonexistent or not needed for good story', 'Oscar (Best Effects - Visual Effects)']
            assert isinstance(result, list)
            assert all(isinstance(tag, str) for tag in result)
            assert result == answer

    
        def test_most_popular(self, tags_obj):
            result = tags_obj.most_popular(3)
            answer = {'In Netflix queue': 17, 'Disney': 16, 'space': 9}
            assert isinstance(result, dict)
            assert all(isinstance(tag, str) for tag in result.keys())
            assert all(isinstance(count, int) for count in result.values())
            # Проверка сортировки по убыванию популярности
            counts = list(result.values())
            assert all(counts[i] >= counts[i+1] for i in range(len(counts)-1))
            assert result == answer


        def test_tags_with(self, tags_obj):
            test_word = "music"
            result = tags_obj.tags_with(test_word)
            answer = ['Music', 'Oscar (Best Music - Original Score)', 'drugs & music', 'good music', 'music']
            assert isinstance(result, list)
            assert all(isinstance(tag, str) for tag in result)
            assert all(test_word.lower() in tag.lower() for tag in result)
            assert result == answer

        def test_top_moveis_by_tag(self, tags_obj):
            movies = Movies(Tests.MOVIES_FILE)
            movies_list = movies.get_movies()
            movie_ids = set(int(m["movieId"]) for m in movies_list)
            ratings_obj = Ratings(Tests.RATINGS_FILE, Tests.MOVIES_FILE, movie_ids)
            result = tags_obj.top_movies_by_tag('In Netflix queue', ratings_obj, movies, 10)
            ans = {'Crumb (1994)': 5.0, 'Once Were Warriors (1994)': 5.0, 'Eat Drink Man Woman (Yin shi nan nu) (1994)': 4.5,
                   'Lone Star (1996)': 4.0, 'When We Were Kings (1996)': 4.0, 'Secret Garden, The (1993)': 3.5, 
                   'My Family (1995)': 3.0}
            assert result == ans


if __name__ == "__main__":
    print("This module is not intended to be run directly. Use it as a library.")
    movies = Movies("../datasets/ml-latest-small/movies.csv")
    movies_list = movies.get_movies()
    movie_ids = set(int(m["movieId"]) for m in movies_list)
    tags = Tags('../datasets/ml-latest-small/tags.csv', movie_ids)
    print(tags.most_words(10))
    print(tags.most_words_and_longest(3))