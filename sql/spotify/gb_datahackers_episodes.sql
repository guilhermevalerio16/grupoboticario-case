SELECT id
     , name
     , description
     , release_date
     , duration_ms
     , languages
     , explicit
     , type
FROM `grupoboticario-case.spotify.spotify_datahackers_episodes_data`
WHERE LOWER(name) LIKE ("%boticário%")
OR LOWER(description) LIKE ("%boticário%")