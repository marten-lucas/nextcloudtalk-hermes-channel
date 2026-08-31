# Restaurierungsplan: Funktionale Parität mit Stand 23.08. (`ee382f9`)

**Basis:** aktueller `main` (`31b0ee4`), modularer Refactor (adapter.py 622 Zeilen + Submodule)
**Referenz:** `ee382f9` (23.08.2026, letzter Stand vor dem Refactor)
**Ziel:** Neue Architektur behalten, fehlende Semantik gezielt auf die Module verteilen. **Kein Rollback.**

## Befund (konsolidiert aus 3 Reviews)

### 🔴 Kritische Regressionen (funktional weg)
| # | Feature | Alt (`ee382f9`) | Neu (`31b0ee4`) |
|---|---------|-----------------|-----------------|
| 1 | Outbound-Kategorisierung (`_categorize_gateway_message`: lifecycle/error/suppress/forward) | ✅ | ❌ komplett weg → Gateway-Rauschen + Loop-Gefahr im Chat |
| 2 | Mention-Gating in Gruppen (`_should_trigger`, `require_mention_in_groups`, `bot_handle`) | ✅ | ❌ Config wird geladen, nie ausgewertet (toter Code) |
| 3 | DM-vs-Group-Erkennung (`chat_type="dm" if participant_count <= 2`) | ✅ | ❌ immer `"group"` |
| 4 | Participant-Count (`_resolve_participant_count`, API-first) | ✅ | ❌ |
| 5 | Context-Fetching (`fetch_last_messages` → `context_messages`) | ✅ | ❌ `context_message_limit` ungenutzt |
| 6 | Attachment-Pipeline im Inbound-Pfad (`extract` + `download` → `attachment_paths`) | ✅ | ❌ `attachment_mgr` existiert, wird nie aufgerufen |
| 7 | Reaction/HITL-Dispatch (`_handle_reaction`, Emoji-Fallback, `request_human_approval`) | ✅ | ❌ `hitl_mgr.handle_reaction` wird nie geroutet |
| 8 | Edit/Delete-Semantik („Nachricht wurde geändert zu…", „wurde geloescht") | ✅ | ❌ nur rudimentäre Delete-Erkennung für SystemMessage-Filter |
| 9 | Message/Session-Korrelation (`_message_index`, `_message_session_keys`, `original_message_id`) | ✅ | ❌ → `replyTo`-Trigger-Kontext und HITL-Cancel kaputt |
| 10 | Command-Normalisierung (`!cmd` → `/cmd` via `_resolve_known_command`) | ✅ | ❌ |
| 11 | `MessageType.COMMAND` vs `TEXT` | ✅ | ❌ immer `TEXT` |
| 12 | WebSocket/HPB-Signaling (`_room_signaling_loop`, `_connect_websocket_once`, Fallback) | ✅ | ❌ `signaling_mgr` instanziiert, nie gestartet; nur Polling |
| 13 | `allowed_rooms`-Filter | ✅ | ❌ geparst, nie geprüft |
| 14 | Fresh-Session-Reset-Note (`_fresh_session_note`) | ✅ | ❌ |

### 🟢 Erhaltene / neue Verbesserungen (behalten!)
- Basis-Loop-Schutz: `sender_id == username`, reservierte Accounts (`system`, `changelog`, `sample`)
- `actorType != "users"`-Filter (neu, besser als alt)
- Native `systemMessage`-Filter (neu)
- Deutsche Systemtext-Filter („Das System hat", „Gesprächseinstellungen verwalten", …) (neu)
- Modulare Struktur: `client/identity/hitl/presence/attachments/signaling`
- `NextcloudOCSException` mit Status-Code-Behandlung in `send_message` (neu)
- Expliziter Poll-Cursor (`lastKnownMessageId`)
- Identity-ContextVars (`X-On-Behalf-Of`, `X-User-Groups`)

### ⚠️ Infrastruktur-Probleme
- Tests: 25 Tests, **nur 1 besteht** (10 Failures, 14 Errors). Ursachen: fehlende Features + fehlende Adapter-Methoden (`request_human_approval`, `send_or_update_status`, `_message_session_keys`) + Import-Struktur (`import adapter` flat vs. relative Imports).
- README/Doku (`docs/nextcloud-talk.md`) beschreiben weiterhin Mention-Trigger, Context, Attachments, HITL, WebSocket-first — Code ist aktuell **weniger** doku-konform als vor dem Refactor.
- `NextcloudRuntimeConfig.attachment_tmp_dir` / `hitl_require_requester` werden nie aus `extra`/Env befüllt.

---

## Umsetzungsplan

### Phase 0 — Test-Infrastruktur reparieren (Voraussetzung)
1. `tests/platforms/nextcloud/test_adapter_contracts.py`: Import-Struktur fixen, damit `import adapter` sowohl flat (Tests) als auch package (Gateway) funktioniert — z. B. Import-Fallback in `adapter.py` (`try: from .client import … / except ImportError: from client import …`) oder Test-Bootstrap, das das Plugin als Package lädt und als `adapter` aliasiert.
2. `python -m unittest discover` als Regressionsnetz etablieren; CI-kompatibel machen.
3. `attachment_tmp_dir` / `hitl_require_requester` aus `extra`/Env befüllen (`NEXTCLOUD_ATTACHMENT_TMP_DIR`, `NEXTCLOUD_HITL_REQUIRE_REQUESTER`).

### Phase 1 — Loop-Prävention & Outbound-Filter (höchste Priorität)
**Ziel-Modul:** neue Datei `outbound.py` (Kategorisierung) + `adapter.py` (Routing)
1. `_categorize_gateway_message()` aus `ee382f9` nach `outbound.py` portieren (lifecycle/error/suppress/forward, inkl. Patterns).
2. `send_message()`-Routing wiederherstellen:
   - `lifecycle` → nur `presence_mgr` (Status/Custom-Message), **kein** Chat-Send
   - `suppress` → still verwerfen (`SendResult(success=True)`)
   - `error` → ohne `reply_to` nur Status „Fehler ⚠️"; mit `reply_to` formatiert (`🚫 **Fehler**`) senden
   - `forward` → normal senden + `signaling_mgr.mark_room_active(room_id)`
3. Zusätzliche Loop-Schutzmaßnahme: Ring-Puffer der letzten N selbst gesendeten Message-IDs; inbound Events mit diesen IDs ignorieren (Absicherung gegen Echo).

### Phase 2 — Inbound-Trigger-Pipeline (Mention, DM/Group, Rooms)
**Ziel-Modul:** `adapter.py` (`handle_incoming_event`) + ggf. `trigger.py`
1. `_resolve_participant_count()` (API-first, Event-Fallback, Default 3) portieren.
2. `_should_trigger(body, participant_count)` portieren (≤2 → immer; >2 → Mention-Pflicht via `bot_handle`-Regex, konfigurierbar).
3. `allowed_rooms`-Check in `handle_incoming_event` (und Polling-Loop) aktivieren.
4. `chat_type` dynamisch: `"dm" if participant_count <= 2 else "group"`.
5. `MessageType.COMMAND` setzen, wenn Body mit `/` beginnt (nach Normalisierung).

### Phase 3 — Event-Semantik (Edit/Delete, Reactions, Attachments, Context)
1. **Reaction-Dispatch:** `eventType`-Erkennung (`reaction` in event_type) → `hitl_mgr.handle_reaction(event, cancel_callback=…)`; danach Return.
2. **Emoji-Reply-Fallback** (`_handle_reaction_fallback_from_message`) portieren.
3. **Edit/Delete:** `is_edit`/`is_delete` aus `eventType`; Body-Umformung („Vergangene Nachricht von {time} wurde geaendert zu: …" / „… wurde geloescht."), `_format_event_time` Helper.
4. **Attachments:** `attachment_mgr.extract_attachments(event)` + Download → `attachment_paths` in das Event-Payload; Empty-Check (`not body.strip() and not attachments → return`).
5. **Context-Fetching:** `fetch_last_messages()` (in `client.py` oder adapter) + bei `participant_count > 2` → `context_messages` ins Payload; `context_message_limit` verwenden.
6. **Command-Normalisierung:** `_normalize_nextcloud_command` + `_resolve_known_command` + `_fallback_known_commands` portieren (in `adapter.py` oder eigenes `commands.py`).

### Phase 4 — Message/Session-Korrelation
1. `_message_index` (original_message_id → text/timestamp) und `_message_session_keys` (→ session_key, requester_user_id, chat_id) wieder einführen.
2. `_build_gateway_session_key()` (via `gateway.session.build_session_key`, mit `group_sessions_per_user`/`thread_sessions_per_user` aus `extra`).
3. `cancel_session_processing` an `_message_session_keys` anbinden (für HITL-Cancel ⛔).
4. `_fresh_session_note()` (Session-Reset-Hinweis bei bestehendem Chat) portieren.

### Phase 5 — WebSocket-Signaling (README-Konformität)
1. `_room_signaling_loop` (ws_connect, hello, join room, Event-Trigger → `_fetch_room_events` → `handle_incoming_event`) in `signaling.py` implementieren.
2. `_connect_websocket_once` + `list_joined_rooms`; Fallback auf Polling wenn WS nicht verfügbar; Polling-Safety-Net parallel laufen lassen.
3. `connect()`/`disconnect()`: WS-Tasks starten/abbrechen, `_leave_room_active` beim Disconnect.
4. Bootstrap-Skip: `_poll_bootstrapped_rooms` tatsächlich verwenden (erste Poll pro Raum: `lookIntoFuture=0` nur zum Cursor-Setzen, keine Events dispatchen).

### Phase 6 — Presence/Status-Contract
1. `send_or_update_status()` und `set_status_text()`-Wiring am Adapter (delegiert an `presence_mgr`), damit Contract-Tests wieder greifen.
2. Lifecycle-States aus Phase 1 an `presence_mgr` anbinden (offline/online/draining).

### Phase 7 — Doku & Version
1. Erst Code an Doku anpassen (oben), danach `docs/nextcloud-talk.md` gegenprüfen und Abweichungen (falls bewusst geändert) dokumentieren.
2. **README komplett neu schreiben** — die aktuelle README beschreibt teils entfernte Features und ist strukturell veraltet. Neue README mit:
   - Aktuellem Feature-Umfang (nach Restaurierung): Outbound-Kategorisierung, Mention-Gating, DM/Group-Erkennung, Attachments, Context-Fetching, Edit/Delete, HITL-Reaktionen, WebSocket-Signaling mit Polling-Fallback, Identity-Header (`X-On-Behalf-Of`, `X-User-Groups`)
   - Vollständiger Konfigurationsreferenz (alle `NEXTCLOUD_*`-Env-Variablen + `extra`-Optionen)
   - Architekturübersicht der Module (`client`, `identity`, `hitl`, `presence`, `attachments`, `signaling`, `outbound`)
   - Installations-/Setup-Anleitung und Beispiel-Platform-Config
   - Verhaltensmatrix (DM vs. Gruppe, System-Messages, Reactions)
3. Version bump (0.2.1), Changelog-Eintrag mit den wiederhergestellten Features.

## Reihenfolge & Abhängigkeiten
```
Phase 0 (Tests) ──► Phase 1 (Loop/Outbound) ──► Phase 2 (Trigger)
                                    │
                                    ▼
              Phase 3 (Events) ──► Phase 4 (Session-State) ──► Phase 5 (WS)
                                                                        │
                                                                        ▼
                                                    Phase 6 (Presence) ──► Phase 7 (Doku/Release)
```
- Phase 1 + 2 beheben den beobachteten Flut-/Loop-Effekt → **sofort nach Phase 0 deploybar**.
- Phasen 3–5 stellen volle Parität mit `ee382f9` her.
- Nach jeder Phase: Test-Suite grün, erst dann weiter.

## Erfolgskriterium
Alle 25 Contract-Tests bestehen; Gateway-Rauschen erscheint nicht mehr im Chat; Gruppen ohne Mention bleiben ruhig; DMs, Attachments, Context, Edit/Delete, HITL-Reaktionen und WebSocket-Signaling verhalten sich wie am 23.08.
