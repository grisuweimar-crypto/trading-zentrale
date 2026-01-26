# =======================================================================
# TAB 1: WATCHLIST (mit Elliott-Integration)
# =======================================================================
with tab_watch:
    # Daten putzen & Standardspalten erstellen
    for c in ['Score', 'MC_Chance', 'Upside', 'PE', 'Typ', 'Beschreibung', 'DivRendite', 'Marge', 'AnalystRec', 'Debt', 'Wachstum', 'Ziel']:
        if c not in df_wl.columns: df_wl[c] = 0
    if 'Akt. Kurs [€]' in df_wl.columns: df_wl = df_wl.rename(columns={'Akt. Kurs [€]': 'Kurs'})
    elif 'Kurs' not in df_wl.columns: df_wl['Kurs'] = 0.0

    # Elliott-Spalten vorbereiten (falls noch nicht vorhanden)
    for c in ['Elliott_Signal', 'Elliott_Confidence', 'Elliott_Entry']:
        if c not in df_wl.columns:
            df_wl[c] = 0 if 'Confidence' in c else "-"
    
    # --- Live Score Berechnung ---
    def calculate_live_score(row):
        score = 0
        typ = str(row['Typ'])
        score += 100 if typ == "DOPPEL" else 80 if typ == "ELLIOTT" else 50
        score += row['MC_Chance'] * (w_mc / 100)
        if 'buy' in str(row['AnalystRec']).lower(): score += w_analyst
        if row['Upside'] > 5: score += w_upside
        elif row['Upside'] < 0: score -= w_upside
        pe = row['PE']
        if 0 < pe < 25: score += w_pe
        elif pe > 60: score -= w_pe 
        debt = row['Debt']
        if debt < 80: score += w_debt
        elif debt > 150: score -= w_debt
        if row['DivRendite'] > 2.0: score += w_div
        if row['Wachstum'] > 5.0: score += w_growth
        if row['Marge'] > 0: score += w_margin
        
        # --- Elliott verstärkend einbeziehen ---
        elliott_conf = float(row.get('Elliott_Confidence', 0))
        score += elliott_conf * 30  # Verstärker, max ~21 Punkte
        
        return score

    df_wl['LiveScore'] = df_wl.apply(calculate_live_score, axis=1)
    
    # Kandidaten filtern
    candidates = df_wl[df_wl['LiveScore'] > 20].copy().sort_values(by='LiveScore', ascending=False)
    
    # --- Elliott Badge ---
    def elliott_badge(confidence):
        if confidence > 0.6:
            return "🟢 Elliott bestätigt"
        elif confidence > 0.4:
            return "🟡 Elliott möglich"
        else:
            return "⚪ kein Elliott"
    
    candidates['Elliott_Badge'] = candidates['Elliott_Confidence'].apply(elliott_badge)
    
    # --- BANNER ---
    c1, c2, c3, c4 = st.columns([1, 1, 2, 1])
    c1.metric("Gescannt", len(df_wl))
    c2.metric("🔥 Treffer", len(candidates))
    
    with c3:
        if not candidates.empty:
            top1 = candidates.iloc[0]['Name']
            top2 = candidates.iloc[1]['Name'] if len(candidates) > 1 else "-"
            top3 = candidates.iloc[2]['Name'] if len(candidates) > 2 else "-"
            
            st.markdown(f"""
            <div style="text-align: center; background-color: #262730; border-radius: 10px; padding: 5px; border: 1px solid #444;">
                <h2 style="margin:0; color: #FFD700; text-shadow: 0px 0px 10px rgba(255, 215, 0, 0.5);">🥇 {top1}</h2>
                <p style="margin:0; font-size: 0.9em; color: #cccccc;">🥈 {top2} &nbsp;&nbsp;|&nbsp;&nbsp; 🥉 {top3}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.metric("🥇 Top Pick", "-")
    
    c4.metric("Ø Chance", f"{candidates['Upside'].mean():.1f}%" if not candidates.empty else "-")
    
    st.divider()
    
    if not candidates.empty:
        # --- CHART & TABELLE ---
        col_chart, col_list = st.columns([1, 2])
        with col_chart:
            st.subheader("💰 Gewinn-Potential")
            top10 = df_wl.sort_values(by='Upside', ascending=False).head(15)
            fig = px.bar(top10, x='Name', y='Upside', color='Upside', 
                         color_continuous_scale=['red', 'yellow', 'green', 'magenta'],
                         title="Analysten Ziele")
            st.plotly_chart(fig, width="stretch")
        
        with col_list:
            st.subheader("📋 Top Kandidaten")
            candidates['PE_Display'] = candidates['PE'].apply(lambda x: "-" if x > 900 else f"{x:.1f}")
            
            st.dataframe(
                candidates[['Name', 'Kurs', 'LiveScore', 'Upside', 'PE_Display', 'DivRendite', 'Elliott_Badge']],
                width="stretch", hide_index=True,
                column_config={
                    "Name": st.column_config.TextColumn("Aktie", help="Name des Unternehmens"),
                    "Kurs": st.column_config.NumberColumn("Preis", format="%.2f €"),
                    "LiveScore": st.column_config.ProgressColumn("Score", format="%d", min_value=0, max_value=200, help="Dein individueller Score (0-200)"),
                    "Upside": st.column_config.NumberColumn("Potential", format="%.1f %%", help="Kurschance bis zum Analystenziel"),
                    "PE_Display": st.column_config.TextColumn("KGV", help="Kurs-Gewinn-Verhältnis (<25 ist gut)"),
                    "DivRendite": st.column_config.NumberColumn("Div %", format="%.2f %%", help="Erwartete Dividendenrendite"),
                    "Elliott_Badge": st.column_config.TextColumn("Elliott", help="Signal laut Elliott-Wave Analyse")
                }
            )
        
        # --- DEEP DIVE ---
        st.divider()
        st.subheader("🔎 Deep Dive")
        for i, (index, row) in enumerate(candidates.head(5).iterrows()):
            score_int = int(row['LiveScore'])
            with st.expander(f"{['🥇','🥈','🥉', '4️⃣', '5️⃣'][i]} {row['Name']} (Score: {score_int})"):
                c_a, c_b, c_c = st.columns(3)
                c_a.info(f"Signal: {row['Typ']} | Elliott: {row['Elliott_Badge']}")
                c_b.success(f"Ziel: {row['Ziel']:.2f}€")
                c_c.warning(f"Upside: {row['Upside']:.1f}%")
                
                desc = str(row['Beschreibung']) if pd.notna(row['Beschreibung']) and row['Beschreibung'] != 0 else "Keine Kurzbeschreibung verfügbar."
                st.markdown(f"**Über das Unternehmen:**\n{desc}")
                st.caption(f"Fundamental: KGV {row['PE_Display']} | Marge {row['Marge']:.1f}% | Wachstum {row['Wachstum']:.1f}% | Schulden {row['Debt']:.0f}%")
