import math

from turnstile import database
from turnstile import utils


def turnstile_filter(global_conf, **local_conf):
    def wrapper(app):
        return TurnstileMiddleware(app, local_conf)
    return wrapper


class TurnstileMiddleware(object):
    def __init__(self, app, local_conf):
        # Save the application
        self.app = app
        self.mapper = None

        # Split up the configuration into groups of related variables
        self.config = {
            None: {
                'status': '413 Request Entity Too Large',
                },
            }
        for key, value in local_conf.items():
            outer, _sep, inner = key.partition('.')

            # Deal with prefix-less keys
            if not inner:
                outer, inner = None, outer

            # Make sure we have a place to put them
            self.config.setdefault(outer, {})
            self.config[outer][inner] = value

        # Set up request preprocessors
        self.preprocessors = []
        for preproc in self.config[None].get('preprocess', '').split():
            # Allow ImportError to bubble up
            self.preprocessors.append(utils.import_class(preproc))

        # Next, let's configure redis
        redis_args = self.config.get('redis', {})
        self.db, self.mapper_daemon = database.initialize(self, redis_args)

    def __call__(self, environ, start_response):
        # Run the request preprocessors
        for preproc in self.preprocessors:
            # Preprocessors are expected to modify the environment;
            # they are helpers to set up variables expected by the
            # limit classes.
            preproc(environ)

        # Now, if we have a mapper, run through it
        if self.mapper:
            self.mapper.routematch(environ=environ)

        # If there were any delays, deal with them
        if 'turnstile.delay' in environ and environ['turnstile.delay']:
            # Find the longest delay
            delay, limit, bucket = sorted(environ['turnstile.delay'],
                                          key=lambda x: x[0])[-1]

            # Set up the retry-after header...
            headers = [('Retry-After', "%d" % math.ceil(delay))]

            # Return the response
            start_response(self.config[None]['status'], headers)
            return limit.format(bucket)

        return self.app(environ, start_response)
