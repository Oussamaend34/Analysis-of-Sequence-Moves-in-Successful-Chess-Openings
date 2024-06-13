from io import StringIO
import chess.pgn
import pandas as pd
import multiprocessing
import re
import chess.pgn
import chess.svg
import os







def get_black_deimer(png_file:str):
    """Extract games with Blackmar-Diemer Gambit from a PGN file."""
    outputfile = png_file.replace('pgn_files/', '')
    outputfile = outputfile.replace('db_standard_rated_', '')
    outputfile = 'Blackmar-Diemer Gambit/' + outputfile
    if os.path.exists(outputfile):
        return outputfile
    pgn = open(png_file)
    black_deimer_gambit =['d2d4', 'd7d5', 'e2e4', 'd5e4', 'b1c3', 'g8f6', 'f2f3', 'e4f3']
    with open(outputfile, 'w') as f:
        pass
    number_of_games = 0
    number_of_games_with_black_deimer_gambit = 0
    while True:
        is_in = False
        game = chess.pgn.read_game(pgn)
        if game is None:
            break
        number_of_games += 1
        if  'Blackmar-Diemer Gambit' not in game.headers['Opening']:
            continue
        if game.headers['ECO'].strip() != 'D00':
            continue
        if game.headers['Result'] != '1-0':
            continue
        for index,move in enumerate(game.mainline_moves()):
            if  index ==  len(black_deimer_gambit):
                is_in = True
                break
            if move.uci() != black_deimer_gambit[index]:
                break
        if is_in:
            number_of_games_with_black_deimer_gambit += 1
            with open(outputfile, 'a') as f:
                f.write(str(game)+'\n\n')
    print(f"Number of games: {number_of_games}")
    print(f"Number of games with Blackmar-Diemer Gambit: {number_of_games_with_black_deimer_gambit}")
    return outputfile


def generate_csv_from_pgn(black_deimer_file:str):
    """Generate a CSV file from a PGN file containing games with Blackmar-Diemer Gambit."""
    pgn_file = open(black_deimer_file)
    csvfile = black_deimer_file.replace('Blackmar-Diemer Gambit/', '')
    csvfile = csvfile.replace('.pgn', '.csv')
    csvfile = 'Data/' + csvfile
    games = {"move 1": [], "move 2": [], "move 3": [], "move 4": [], "move 5": [], "move 6": [], "move 7": [], "move 8": [], "move 9": [], "move 10": [], "move 11": [], "move 12": []}

    while True:
        pgn = chess.pgn.read_game(pgn_file)
        if pgn is None:
            break
        pgn_str = str(pgn)
        moves:str = pgn_str.splitlines()[-1]
        pattern = r'\{.*?\}'
        to_clean:list = re.findall(pattern, moves)
        pattern = r' \$\d+'
        to_clean.extend(re.findall(pattern, moves))
        pattern = r' \d+\.\.\. '
        to_clean.extend(re.findall(pattern, moves))
        for clean in to_clean:
            moves:str = moves.replace(clean, '')
        pattern = r'([BNKQR]?[a-h]?[1-8]?x?[a-h][1-8][\+#]?|O-O|O-O-O) ([BNKQR]?[a-h]?[1-8]?x?[a-h][1-8][\+#]?|O-O|O-O-O)?'
        moves:list[str] = re.findall(pattern, moves)
        for i,move in enumerate(moves.copy()):
            moves[i] = (move[0] + ' ' + move[1]).strip()
        if len(moves) < 12:
            continue
        moves = moves[0:12]
        for i in range(len(moves)):
            games[f"move {i + 1}"].append(moves[i])
    games = pd.DataFrame(games)
    print(games)
    games.to_csv(csvfile, index=False)


def proccess(pgn_file:str):
    """Process a PGN file to extract games with Blackmar-Diemer Gambit and generate a CSV file with the moves using multiprocessing."""
    print(f"Processing file: {pgn_file}")
    outputfile = get_black_deimer(pgn_file)
    generate_csv_from_pgn(outputfile)

def combine_all_data():
    games:pd.DataFrame = pd.DataFrame({"move 1": [], "move 2": [], "move 3": [], "move 4": [], "move 5": [], "move 6": [], "move 7": [], "move 8": [], "move 9": [], "move 10": [], "move 11": [], "move 12": []})
    for file in os.listdir('Data'):
        if "alldata" in file:
            continue
        if file.endswith('.csv'):
            csv_file = f"Data/{file}"
            _ = pd.read_csv(csv_file)
            games = pd.concat([games, _], ignore_index=True)
    print("Before drop duplicates")
    print(games.shape)
    games.drop_duplicates(inplace=True)
    print("After drop duplicates")
    print(games.shape)
    games.to_csv('Data/Black-Diemer Gambit-alldata.csv', index=False)
    print("before drop invalid moves")
    print(games.shape)
    pattern = r'([BNKQR]?[a-h]?[1-8]?x?[a-h][1-8][\+#]?|O-O|O-O-O) ([BNKQR]?[a-h]?[1-8]?x?[a-h][1-8][\+#]?|O-O|O-O-O)'
    games = games[games['move 12'].str.match(pattern)]
    print("after drop invalid moves")
    print(games.shape)
    games.to_csv('Data/Black-Diemer Gambit-alldata-test.csv', index=False)
if __name__ == "__main__":
    files = [ 'pgn_files/lichess_db_standard_rated_2014-10.pgn', 'pgn_files/lichess_db_standard_rated_2014-11.pgn','pgn_files/lichess_db_standard_rated_2014-12.pgn']
    # png_file = "pgn_files/lichess_db_standard_rated_2014-04.pgn"
    num_processes = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=num_processes)
    pool.map(proccess, files)
    pool.close()
    pool.join()
    combine_all_data()
