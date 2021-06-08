def consultauser_login():
    return """
        select a.user_login,
            a.created_at,
            a.updated_at,
            (julianday(date('now')) - julianday(a.updated_at)) as diferenca
        from pulls a
        where a.user_login in (
                'tobiasdiez',
                'koppor',
                'matthiasgeiger',
                'Siedlerchr',
                'simonharrer',
                'mortenalver',
                'oscargus'
            )
        order by user_login,
            a.updated_at
   
    """
