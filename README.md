# metaphor
Library for creating and serving RESTful APIs

Metaphor is intended as an exercise in [System Metaphor](https://wiki.c2.com/?SystemMetaphor).

Using the schema editor a domain expert may construct their view of their own domain.
This may then be immediately interacted with through a RESTful API.

This library works best for calculation heavy domains, and includes a basic calc language.

Resource structure:
 - A schema may define many resources
 - A resource may define fields of various types
 - A resource may contain other resources as fields
 - A resource may contain links to other resources as fields
 - A resource may contain calculations as fields

Calculations:
 - Calculations can start at the parent resource, or the root of the API. i.e. "self.sales"
 - Calculations may include basic aggregate functions around aggregate types. i.e. "average(doctors.salary)"
 - Calculations will automatically update when a dependent resource is altered.
 
Local setup:

Mongo DB is required. Note: It's even required to run the unit tests. There are a few reasons for this.

Code setup:
clone this project, then (using virtualenvwrapper) run:
```
mkvirtualenv metaphor
pip install -r requirements.txt
```
There is a test server script in the root, which sets up the environment variables used by the Flask app:
```
./server.sh
```
Then, navigate to [http://localhost:8000/admin/schema_editor](http://localhost:8000/admin/schema_editor)

Here you can create new resources, add them to the root, and add fields to each resource.

Having done that, navigate to [http://localhost:8000/browser/](http://localhost:8000/browser/)

Here resources can be interacted with.

The api proper is located at: [http://localhost:8000/api/](http://localhost:8000/api/)
