scala-jdbc
==========

Scala functional wrapper for JDBC API. There are additional materials available that support this repository and detail its contents:
* [Presentation Slides][slides]
* [2013 Blog Post][blog-post]
* [2013 Philly JUG/PHASE Presentation][presentation-video]:

NOTE: This repository was updated in 2019 to upgrade all of its dependencies in response to GitHub alerts.

Getting started tips:
* You do not need gradle installed to build this project.  Just clone the repository build it
* This project requires *exactly* JDK8. The version of Scala used by this project does not work with later JDK versions.
* gradlew test run  # Runs the unit tests and example program
* gradlew scaladoc  # Generates project documentation

[slides]: https://martinsnyder.net/asset/revealjs/scala-jdbc.html
[blog-post]: https://martinsnyder.net/blog/2013/08/07/functional-wrappers-for-legacy-apis/
[presentation-video]: https://vimeo.com/75591447