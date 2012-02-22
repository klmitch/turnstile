==============================================
Turnstile Distributed Rate-Limiting Middleware
==============================================

Turnstile is a piece of WSGI middleware that performs true distributed
rate-limiting.  System administrators can run an API on multiple
nodes, then place this middleware in the pipeline prior to the
application.  Turnstile uses a Redis database to track the rate at
which users are hitting the API, and can then apply configured rate
limits, even if each request was made against a different API node.
