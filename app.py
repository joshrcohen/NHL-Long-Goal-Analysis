from flask import Flask, render_template, request, jsonify, redirect
from dataclasses import dataclass
import src.DataFetcher as df
import datetime
from sqlalchemy import MetaData
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship, backref
from flask_migrate import Migrate, migrate
import os
import math
import threading

from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'


convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
metadata = MetaData(naming_convention=convention)
db = SQLAlchemy(app, metadata=metadata)

app.app_context().push()



migrate = Migrate(app, db, render_as_batch=True)


@app.context_processor
def inject_global_vars():
    return {
        'display_limit': 10
    }

@dataclass
class Plays(db.Model):
    __tablename__ = 'plays'
    id = db.Column(db.Integer, primary_key=True)
    event_id = db.Column(db.String)
    period = db.Column(db.String)
    time_in_period = db.Column(db.String)
    time_remaining = db.Column(db.String)
    situation_code = db.Column(db.String)
    home_team_defending_side = db.Column(db.String)
    type_code = db.Column(db.String)
    type_desc_key = db.Column(db.String)
    sort_order = db.Column(db.String)

    match = db.Column(db.Integer, db.ForeignKey("match.id"))

    def to_dict(self):
        return {field.name:getattr(self, field.name) for field in self.__table__.c}

    
@dataclass
class Season(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    season = db.Column(db.String, unique=True)
    season_display_value = db.Column(db.String)
    #season_starting_year = db.Column(db.String, unique=True)

    def to_dict(self):
        return {field.name:getattr(self, field.name) for field in self.__table__.c}

@dataclass
class Match(db.Model):
    __tablename__ = 'match'
    id = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(db.String, unique=True, nullable=False)
    season = db.Column(db.String)
    game_type = db.Column(db.String)
    game_date = db.Column(db.Date)
    start_time = db.Column(db.DateTime)
    game_state = db.Column(db.String)
    game_schedule_state = db.Column(db.String)
    period = db.Column(db.String)
    display_period = db.Column(db.String)
    home_team_score = db.Column(db.Integer)
    away_team_score = db.Column(db.Integer)

    plays = relationship("Plays")

    home_team_id = db.Column(db.Integer, db.ForeignKey("team.team_id"))
    away_team_id = db.Column(db.Integer, db.ForeignKey("team.team_id"))

    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])

    def to_dict(self):
        return {field.name:getattr(self, field.name) for field in self.__table__.c}

@dataclass
class Team(db.Model):
    __tablename__ = 'team'
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.String)
    name = db.Column(db.String)
    abbrev = db.Column(db.String)
    logo = db.Column(db.String)

    # home_team_id = db.Column(db.Integer, db.ForeignKey('match.id'))
    # away_team_id = db.Column(db.Integer, db.ForeignKey('match.id'))
    # home_team = relationship("Match", foreign_keys=[home_team_id], backref="home_team")
    # away_team = relationship("Match", foreign_keys=[away_team_id], backref="away_team")


    def to_dict(self):
        return {field.name:getattr(self, field.name) for field in self.__table__.c}

# Season games mapping
SEASONS_GAMES_MAPPING = {
    # Add entries for each season with the corresponding number of games 
    "2009": 1230,
    "2010": 1230,
    "2011": 1230,
    "2012": 720,
    "2013": 1230,
    "2014": 1230,
    "2015": 1230,
    "2016": 1230,
    "2017": 1230,
    "2018": 1230,
    "2019": 1082,
    "2020": 868,
    "2021": 1312,
    "2022": 1312,
}

BATCH_SIZE = 1312
PAGE = 1

@staticmethod
def generate_game_ids_for_season(season):
    total_games = SEASONS_GAMES_MAPPING[season]
    season_game_data = []
    game_id = [game_id for game_id in range(1, total_games+1)]
    return game_id  


@staticmethod
def to_json(row):
    arr = []
    for u in row:
        arr.append(u.to_dict())
    return arr


@app.route('/delete', methods=['POST'])
async def erase():
    matches = Match.query.all()
    for match in matches:
        db.session.delete(match)
        db.session.commit()
    return redirect('/')

@app.route("/season/<season>/<page>", methods=['GET'])
async def get_season_data(season, page):
    if request.method == 'GET':
        data = None
        game = None 

        count = Match.query.filter_by(season=season+str(int(season)+1)).count()
        if count is not None and  int(page) < 1 :
            return redirect('/season/'+season + '/'+str(PAGE))
        if count > 0 and int(page) > math.ceil(count/10):
             return redirect('/season/'+season + '/'+str(math.ceil(count/10)))
        
        temp = Match.query.filter_by(season=season+str(int(season)+1)).first()
        if temp is None:
            thread = threading.Thread(target= await insertMatches(season))
            thread.daemon = True
            thread.start()
            
        
        return render_template('body.html', 
            matches=to_json(Match.query.filter_by(season=season+str(int(season)+1)).paginate(page=int(page),per_page=10)), 
            seasons=to_json(Season.query.order_by('season').all()),
            pages=int(page),
            current_season=season,
            count=math.ceil(count/10),
            current_page=int(page)
        )
    

@staticmethod
async def insertMatches(season):
    full_season = season+str(int(season)+1)
    seasonRecord = Season.query.filter_by(season=full_season).first()
        
    if seasonRecord is None:
        seasonRecord = Season(
            season=full_season,
            season_display_value=full_season[0:4] + '-' + full_season[4:8]
        )
        db.session.add(seasonRecord)
        db.session.commit()

    for game_id in generate_game_ids_for_season(season):
        formatted_game_id = f"{season}02{game_id:04d}"
        current_game = Match.query.filter_by(match_id=formatted_game_id).first()
        if current_game is None:
            data = await df.DataFetcher.fetch_game_data(formatted_game_id)

            

            home_team = Team.query.filter_by(team_id=data["homeTeam"]["id"]).first()
            if home_team is None:
                home_team = Team(
                    team_id=data["homeTeam"]["id"],
                    name=data["homeTeam"]["name"]["default"],
                    abbrev=data["homeTeam"]["abbrev"],
                    logo=data["homeTeam"]["logo"],
                )
                db.session.add(home_team)
                db.session.commit()
            print(home_team)
            away_team = Team.query.filter_by(team_id=data["awayTeam"]["id"]).first()
            if away_team is None:
                away_team = Team(
                    team_id=data["awayTeam"]["id"],
                    name=data["awayTeam"]["name"]["default"],
                    abbrev=data["awayTeam"]["abbrev"],
                    logo=data["awayTeam"]["logo"],
                )
                db.session.add(away_team)
                db.session.commit()

            game = Match(
                match_id=formatted_game_id,
                season=data["season"],
                game_type=data["gameType"],
                game_date=datetime.datetime.strptime(data["gameDate"], '%Y-%m-%d'),
                start_time=datetime.datetime.strptime(data["startTimeUTC"], '%Y-%m-%dT%H:%M:%SZ'),
                game_state=data["gameState"],
                game_schedule_state=data["gameScheduleState"],
                period=data["period"],
                display_period=data["displayPeriod"],
                home_team=home_team,
                away_team=away_team,
                home_team_score=data["homeTeam"]["score"],
                away_team_score=data["awayTeam"]["score"],
            )
            db.session.add(game)
            db.session.commit()

@app.route("/", methods=['GET', 'POST'])
async def index():
    if(request.method == 'GET'):
        form_data = request.args.get('season')
        if form_data:
            return redirect('season/'+form_data + '/'+str(PAGE))
        else:
            return render_template('body.html', seasons=to_json(Season.query.order_by('season').all()))
    return redirect('/')

@app.route("/match/<match_id>/<page>", methods=['GET'])
async def match(match_id, page):

    count = Plays.query.filter_by(match=match_id).count()
    if count is not None and  int(page) < 1 :
        return redirect('/match/'+match_id + '/'+str(PAGE))
    if count > 0 and int(page) > math.ceil(count/10):
        return redirect('/season/'+match_id + '/'+str(math.ceil(count/10)))
        
    game = Match.query.filter_by(match_id=match_id).first()
    plays = Plays.query.filter_by(match=match_id).first()

    if plays is None:
        thread = threading.Thread(target=await insertPlays(match_id))
        thread.daemon = True
        thread.start()
    
    return render_template(
        "match.html",
        match_id=match_id, 
        matches=game,
        plays=to_json(Plays.query.filter_by(match=match_id).paginate(page=int(page),per_page=10)),
        count=math.ceil(count/10),
        current_page=int(page)
    )

@staticmethod
async def insertPlays(match_id):
    data = await df.DataFetcher.fetch_game_data(match_id)
    for current_play in data["plays"]:
        play = Plays(
            event_id = current_play["eventId"],
            period = current_play["period"],
            time_in_period = current_play["timeInPeriod"],
            time_remaining = current_play["timeRemaining"],
            home_team_defending_side = current_play["homeTeamDefendingSide"] if "homeTeamDefendingSide" in current_play else "",
            type_code = current_play["typeCode"],
            type_desc_key = current_play["typeDescKey"],
            sort_order = current_play["sortOrder"],
            match=match_id
        )
        db.session.add(play)
        db.session.commit()
    #return Plays.query.filter_by(match=match_id).all()


if __name__ == '__main__':
    app.run(threaded=True)