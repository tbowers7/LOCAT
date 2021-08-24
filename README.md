# LOCAT

Package to add new pointing catalogs to the Lowell Observatory CAT.

Public catalogs supported by this package:

* `Gaia EDR3` - Gaia Early Data Release 3

---

## General Outline of What Needs to Happen:


*From Georgi:*

Hello y'all

Two things:

(1) Dyer and I found out that there is MySQL running on jumar with all
catalogs, so we should be able to test any new catalogs with CAT locally
on the Hill

(2) I wrote to Saeid regarding the conversion of text catalogs to SQL
databases, and this is what he replied:

---
I found it and it is DatabaseMain.java in the CAT plugin. It is a
dangerous program to run as it will overwrite your database if you make
a mistake. Most of it is commented out because each time it needs to be
customized for the specific catalog but this should give you a head
start. My suggestion is to run it against a test MySQL instance and
after you have populated your tables properly, do an SQL export and then
import it into the main database.

There is a database package in the plugin that has the helper files.

Good luck,
Saeid

---

I want to add to that that there are test databases on joe, but we
should avoid any interaction with joe until we're sure that the database
is created properly and can be used by CAT. Let's do all this locally
first on dct-sim1 (for CAT) and jumar (for the databases and MCB).

Georgi

---
*From Dyer:*

Hi Tim,

I’ve never looked into this.  Perhaps some exploring in the database provided by Ryan will be helpful.
I don’t know how catalogs get into the database.  There is an entry in the subversion archive on jumar
called “CatalogDataPrototype” although I haven’t looked at what is in it.

 -Dyer
