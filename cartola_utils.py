import requests

def buscar_parciais_globais(headers):
    """Busca as pontuações ao vivo de todos os jogadores."""
    try:
        res = requests.get("https://api.cartola.globo.com/atletas/pontuados", headers=headers, timeout=30).json()
        return {int(id_str): info.get('pontuacao', 0.0) for id_str, info in res.get('atletas', {}).items()}
    except Exception as e:
        print(f"⚠️ Erro ao buscar parciais globais: {e}")
        return {}

def buscar_status_partidas(headers):
    """Mapeia o status das partidas por clube_id (ex: ENCERRADA, EM_ANDAMENTO)."""
    try:
        res = requests.get("https://api.cartola.globo.com/partidas", headers=headers, timeout=30).json()
        m = {}
        for p in res.get('partidas', []):
            s = p.get('status_transmissao_tr', 'DESCONHECIDO')
            m[p['clube_casa_id']] = s
            m[p['clube_visitante_id']] = s
        return m
    except Exception as e:
        print(f"⚠️ Erro ao buscar status das partidas: {e}")
        return {}

def calcular_parciais_equipe(time_id, mapa_pontos, mapa_status, headers):
    """
    Calcula a pontuação da equipe em tempo real, aplicando as regras de banco de reservas e reserva de luxo.
    Retorna uma tupla: (total_pontos_equipe, lista_de_titulares_atualizada)
    """
    if not time_id or str(time_id) == "0": 
        return 0.0, []
    
    try:
        url = f"https://api.cartola.globo.com/time/id/{time_id}"
        d = requests.get(url, headers=headers, timeout=30).json()
        tits = []
        
        # 1. Monta o time titular original
        capitao_id = d.get('capitao_id')
        for t in d.get('atletas', []):
            pid = t['atleta_id']
            tits.append({
                'id': pid, 
                'apelido': t.get('apelido', f'Atleta {pid}'),
                'pos': t['posicao_id'], 
                'club': t['clube_id'], 
                'pts': mapa_pontos.get(pid, 0.0), 
                'cap': (pid == capitao_id)
            })
        
        reservas = d.get('reservas', [])
        
        # 2. Lógica de Substituição Padrão
        for i, t in enumerate(tits):
            if mapa_status.get(t['club']) == "ENCERRADA" and t['pts'] == 0.0:
                r = next((x for x in reservas if x['posicao_id'] == t['pos'] and mapa_pontos.get(x['atleta_id'], 0.0) != 0.0), None)
                if r:
                    tits[i].update({
                        'id': r['atleta_id'], 
                        'apelido': r.get('apelido', f"Reserva {r['atleta_id']}"),
                        'pts': mapa_pontos.get(r['atleta_id'], 0.0)
                    })
                    reservas.remove(r)

        # 3. Lógica do Reserva de Luxo
        lux_id = d.get('reserva_luxo_id')
        if lux_id:
            lx = next((r for r in reservas if r['atleta_id'] == lux_id), None)
            if lx:
                concs = [t for t in tits if t['pos'] == lx['posicao_id']]
                if concs and all(mapa_status.get(t['club']) == "ENCERRADA" for t in concs):
                    pior = min(concs, key=lambda x: x['pts'])
                    pts_luxo = mapa_pontos.get(lux_id, 0.0)
                    if pts_luxo > pior['pts']:
                        tits[tits.index(pior)].update({
                            'id': lx['atleta_id'],
                            'apelido': lx.get('apelido', f"Luxo {lux_id}"),
                            'pts': pts_luxo
                        })

        # 4. Calcula total aplicando o multiplicador do capitão
        total = round(sum((t['pts'] * 1.5 if t['cap'] else t['pts']) for t in tits), 2)
        
        return total, tits
    except Exception as e: 
        print(f"⚠️ Erro ao calcular equipe {time_id}: {e}")
        return 0.0, []
