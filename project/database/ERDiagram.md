```mermaid
erDiagram
    PARTICIPANTS ||--|{ ACCIDENT : "involved"
    COORDINATE ||--|{ ACCIDENT : "location"

    class PARTICIPANTS {
        bool predestrian
        bool car
        bool truck
        bool motorcycle
        bool bike
        bool other
    }

    class COORDINATE {
        int utm_zone
        decimal utm_x
        decimal utm_y
        decimal wsg_long
        decimal wsg_lat
    }

    class ACCIDENT {
        int road_state
        int severeness
        int lighting_conditions
        int year
        int month
        int hour
        int weekday
        string road_type_osm
        string road_type_parsed
    }

```