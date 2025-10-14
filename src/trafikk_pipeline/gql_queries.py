REGISTRATION_POINTS_VESTLAND = """
query RegistrationPointsVestland {
  trafficRegistrationPoints(
    searchQuery: { roadCategoryIds: [R], countyNumbers: [46], isOperational: true }
  ) {
    id
    name
    location { coordinates { latLon { lat lon } } }
  }
}
"""

VOLUME_BY_DAY = lambda pid, start_iso, end_iso: f"""
query trafficvolume {{
  trafficData(trafficRegistrationPointId: "{pid}") {{
    volume {{
      byDay(
        from: "{start_iso}"
        to:   "{end_iso}"
        first: 100
      ) {{
        edges {{
          node {{
            from
            to
            total {{ volumeNumbers {{ volume }} }}
          }}
        }}
      }}
    }}
  }}
}}
"""
