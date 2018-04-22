from flask import Flask, request

user_agent = 'osm-wikidata/0.1 (https://github.com/EdwardBetts/osm-wikidata; edward@4angle.com)'

default_change_comments = {
    'single': 'add wikidata tag',
    'multi': 'add wikidata tags within PLACE',
}

class MatcherFlask(Flask):
    def log_exception(self, exc_info):
        self.logger.error("""
Path:                 %s
HTTP Method:          %s
Client IP Address:    %s
User Agent:           %s
User Platform:        %s
User Browser:         %s
User Browser Version: %s
GET args:             %s
view args:            %s
URL:                  %s
""" % (
            request.path,
            request.method,
            request.remote_addr,
            request.user_agent.string,
            request.user_agent.platform,
            request.user_agent.browser,
            request.user_agent.version,
            dict(request.args),
            request.view_args,
            request.url
        ), exc_info=exc_info)

def user_agent_headers():
    return {'User-Agent': user_agent}
