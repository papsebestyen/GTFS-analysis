# GTFS-analysis

This repository contains a GTFS analysis tool capable of estimating the **most optimal travel time** measures between two transit stops or cities.

## Most optimal travel time

Optimal travel time means that every edge on the transit network (directed multigraph) which connects a start and end stop was calculated with the minimum of all possible routes between the two stops regardless of the time. In contrast, real travel time depends on the timing of a given trip. (day of the week and time of the day)

Important to highlight that this calculation is very different than the estimated time of standard route planning applications because the optimal travel time neither calculates with transfer time nor with the time-varying feature of transport networks.

## Walking estimates

Two nodes are connected with an edge in the transit network if the distance between them is less than 200 meters. The weights of these edges are estimations of [preferred walking speed](https://en.wikipedia.org/wiki/Preferred_walking_speed).
 
# Data sources

The GTFS data collected in Sept of 2022 from the following pages:
 - [MÁV](https://www.mavcsoport.hu/gtfs-igenybejelento) (required request)
 - [Volán](https://www.volanbusz.hu/hu/menetrendek/gtfs)
 - [BKV](https://bkk.hu/fejlesztesek/fejlesztoknek/)



