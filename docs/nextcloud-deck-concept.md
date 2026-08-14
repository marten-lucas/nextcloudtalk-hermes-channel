# Nextcloud Deck Konzept fuer Hermes

## Stand nach Anforderungsabgleich

Dieses Konzept ersetzt die fruehere Idee einer kombinierten Deck- und Tasks-Integration.

Festgelegt ist jetzt:

- Hermes bekommt Aufgaben primaer ueber **Nextcloud Deck**
- Trigger entstehen bei **Aenderungen an Karten, die Hermes zugewiesen sind**
- Hermes arbeitet als **fester Nextcloud-User**, nicht im Kontext des zuletzt aendernden Menschen
- Hermes darf in Deck:
  - Kommentare schreiben
  - Karten zwischen Spalten verschieben
  - Checkboxen in der Kartenbeschreibung abhaken und ergaenzen
- Eine Spiegelung nach **Nextcloud Talk** soll **pro Board konfigurierbar** sein
- Eine direkte Integration der **Nextcloud Tasks App** ist vorerst **nicht noetig**, weil der gewuenschte User-Flow ueber Deck ausreicht

## Erkenntnisse aus der Recherche

### Bestehender Nextcloud-Adapter

Der aktuelle Adapter in [adapter.py](</home/marten/Development/kiga AI/hermes-nextcloud-channel/adapter.py>) ist ein reiner **Nextcloud Talk**-Adapter:

- Eingang ueber Chat-Events und Reaktionen
- Ausgang ueber Talk-Nachrichten mit `replyTo`
- Praesenz-, Status- und Typing-Signale
- Identitaet des Hermes-Bot-Users ist fest und OCS-basiert

Die bestehende Talk-Integration ist damit eine gute Basis fuer:

- optionale Rueckmeldungen in Talk
- Wiederverwendung von HTTP-/OCS-Hilfslogik
- spaetere gemeinsame Nextcloud-Basisbausteine

Sie ist aber **kein gutes Heim fuer Deck-spezifische Inbound-Logik**, weil Deck-Karten keine Chat-Nachrichten sind und als Session-Quelle getrennt sichtbar sein sollen.

### Deck-Faehigkeiten

Deck bietet eine dokumentierte REST-API fuer:

- Boards
- Stacks
- Cards
- Assignments
- Labels
- Kommentare
- Attachments

Kartenbeschreibungen sind Markdown. Checklisten werden dort als Markdown-Checkboxen modelliert. Eine separate Checklist-API wurde in der Recherche nicht gefunden.

### Vergleich mit Hermes-Upstream

- Die **Microsoft Teams**-Integration ist eine Messaging-Integration, keine Planner-Integration. Sie verarbeitet Teams-Chats, Kanaele, Adaptive Cards und optional Meeting-Summaries, aber nicht Microsoft Planner.
- Es wurde **keine bestehende Hermes-Plattform fuer Trello, Planner oder Jira** als direkte Plattformintegration gefunden.
- Es gibt jedoch eine **interne Hermes-Kanban-Funktion** im Desktop-Bereich, die mit klaren Statussaeulen und Agent-Orchestrierung arbeitet. Diese ist eher ein internes Workflow-Modell als eine externe SaaS-Integration.

## Architekturentscheidung

## Empfehlung: eigene Plattform `nextcloud-deck`

Statt den bestehenden Talk-Adapter zu ueberladen, sollte Deck als **eigene Plattform** modelliert werden.

### Gruende

1. **Korrekte Session-Quelle**
   - In Hermes soll die Quelle als Deck-Arbeitsobjekt sichtbar sein, nicht als Talk-Chat.
   - Beispiel: `nextcloud-deck:board-12/card-481`

2. **Saubere Semantik**
   - Talk = Konversation
   - Deck = Work Item / Kanban-Karte

3. **Bessere Weiterentwicklung**
   - Polling, Mapping, Loop-Schutz und Pairing unterscheiden sich stark von Talk
   - Tests und Konfiguration bleiben klarer

4. **Trotzdem Wiederverwendung**
   - HTTP-/Auth-/Nextcloud-Clientlogik kann spaeter in gemeinsame Hilfsbausteine ausgelagert werden

## Nicht empfohlen

- Alles in den bestehenden Talk-Adapter hineinzubauen
  - wuerde Source-Semantik verwischen
  - mischt Chat- und Work-Item-Lebenszyklen
  - erschwert Tests und Fehlersuche

## Zielbild

Zwei getrennte, aber verwandte Plattformen:

- `nextcloud-talk`
- `nextcloud-deck`

Optional kann spaeter eine gemeinsame interne Basis `nextcloud_common` entstehen.

## Beobachtungsmodell

## Welche Boards werden beobachtet?

Nicht manuell vorkonfigurierte Boards, sondern **dynamische Entdeckung**:

- Hermes liest die Boards, in denen der Hermes-User Mitglied ist
- aktiv verarbeitet werden aber nur **Karten, die Hermes zugewiesen sind**

Damit ist keine statische Liste "beobachteter Boards" noetig.

### Persistiert werden nur boardspezifische Einstellungen

Pro Board werden nur Dinge gespeichert wie:

- Pairing abgeschlossen ja/nein
- Stack-Mapping
- Talk-Mirroring an/aus
- optionale Ziel-Raum-ID fuer Talk-Mirroring

## Trigger-Modell

Ein Hermes-Lauf wird gestartet, wenn sich eine Hermes-zugewiesene Karte relevant aendert, z. B.:

- Titel
- Beschreibung
- Checkbox-Zustand
- Kommentare
- Labels
- Due Date
- Zuweisungen
- Stack / Position

### Wichtige Trigger-Regel

Nicht die Board-Mitgliedschaft triggert den Agenten, sondern die **Kartenzuweisung an Hermes**.

## Rueckschreibemodell

Hermes darf in Deck:

- Kommentare posten
- Karten in andere Spalten verschieben
- Checkboxen in der Beschreibung aendern

### Beschreibungsstrategie

Da Checkboxen wichtiger sind als der Schutz der Beschreibung, wird die Beschreibung nicht mehr als schreibgeschuetzt behandelt.

Trotzdem sollte die Implementierung moeglichst defensiv sein:

- Checkbox-Zeilen gezielt aendern statt die gesamte Beschreibung neu zu erzeugen
- Optimistic concurrency mit `lastModified`/ETag
- Konflikte sichtbar loggen statt still zu ueberschreiben

Fuer Freitext-Status und Notizen reichen **Deck-Kommentare**; ein eigener Hermes-Block in der Beschreibung ist damit nicht mehr noetig.

## Talk-Integration im Zielbild

Talk bleibt ein **optionaler Spiegelkanal**, nicht die primaere Quelle.

Pro Board konfigurierbar:

- `mirror_to_talk = true|false`
- optional `talk_room_id`

### Empfohlener Ablauf

- Deck ist die fuehrende Quelle
- Hermes verarbeitet Deck-Karte
- Rueckmeldung erfolgt immer auf der Karte per Kommentar
- zusaetzlich optional kurze Statusmeldung in Talk

## Stack-Mapping / Statusmodell

## Nicht per Prompt bei jeder Aenderung

Das Stack-Mapping sollte **nicht** bei jedem Lauf per freiem Prompt neu bestimmt werden.

## Stattdessen: Board-Pairing

Wenn Hermes ein neues Board zum ersten Mal sinnvoll bearbeiten soll, startet ein **Board-Pairing-Flow**.

Ausloeser:

- Hermes wurde neu einem Board hinzugefuegt und das Board ist noch unbekannt
- oder Hermes sieht die erste zugewiesene Karte in einem noch ungepairten Board

### Ziel des Pairings

Abbildung der konkreten Deck-Spalten auf ein kanonisches Hermes-Statusmodell, z. B.:

- inbox
- todo
- in_progress
- blocked
- review
- done

Nicht jedes Board muss jede kanonische Spalte besitzen.

### Ergebnis

Persistierte Board-Konfiguration, z. B. sinngemaess:

```yaml
board_id: 12
paired: true
mirror_to_talk: false
status_map:
  todo: "Backlog"
  in_progress: "In Arbeit"
  blocked: "Blockiert"
  review: "Review"
  done: "Erledigt"
```

## Pairing-UX

### Empfehlung

Den bestehenden Hermes-**Pairing-Gedanken** wiederverwenden, aber nicht zwingend die bestehende DM-Pairing-Implementierung 1:1.

Die vorhandene Hermes-Pairing-Logik ist eigentlich fuer **Autorisierung unbekannter Messaging-Nutzer** gebaut. Fuer Deck brauchen wir stattdessen ein **Board-Onboarding**.

### Sinnvolle UX-Form

Wenn ein Board ungepairt ist:

- Hermes kommentiert auf der ersten betroffenen Karte oder sendet optional in Talk:
  - dass das Board noch nicht gepairt ist
  - welche Spalten gefunden wurden
  - welche Zuordnung Hermes vorschlaegt
- der Mensch bestaetigt oder korrigiert die Zuordnung
- danach wird die Zuordnung gespeichert

### Heuristik vor Rueckfrage

Hermes darf zuerst offensichtliche Namen vorschlagen:

- `todo`, `backlog`, `offen` -> `todo`
- `doing`, `in progress`, `in arbeit` -> `in_progress`
- `blocked` -> `blocked`
- `review` -> `review`
- `done`, `erledigt`, `fertig` -> `done`

Wenn die Zuordnung nicht eindeutig ist, bleibt das Board im Status **pairing_required**.

## Rolle der Hermes-Kanban-Funktion

Die interne Hermes-Kanban-Funktion kann fuer dieses Feature hilfreich sein, aber eher **als Denkmodell** als als direkte technische Abhaengigkeit.

### Sinnvoll nutzbar

- als kanonisches Statusmodell
- als Quelle fuer Presets und Benennungen
- spaeter eventuell fuer Import/Export zwischen internem Hermes-Kanban und Deck

### Nicht sinnvoll als erste Abhaengigkeit

- direkte technische Kopplung an Desktop-spezifische Kanban-UI oder deren interne Datenmodelle
- Vermischung von internem Orchestrierungsboard und externem Deck-Board in Phase 1

## Vereinfachte Konfiguration

Nach der Scope-Reduktion bleibt nur noch wenig Pflichtkonfiguration:

- Nextcloud Base URL
- Hermes Username
- App Password

Plus optionale Deck-spezifische Einstellungen:

- Poll-Intervall
- Talk-Mirroring Default
- optional Standard-Talk-Raum

Wichtig: Die frueher diskutierten Punkte

- "Hermes-Zuordnungskennung"
- "Assignee/Participant-Matching"

sind nach Entfall der direkten Tasks-Integration nicht mehr getrennt noetig.

Es bleibt im Kern nur:

- **Wie erkennt Deck, dass Hermes zugewiesen ist?**
  - Antwort: ueber den festen Nextcloud-Hermes-User in `assignedUsers`

## Technischer Umsetzungsplan

### Phase 1 - Architekturgrundlage

- neues Plattform-Plugin `nextcloud-deck` anlegen
- gemeinsame Nextcloud-Clienthilfen aus Talk-Adapter extrahierbar machen
- Session-Key-Schema fuer Deck-Karten definieren

### Phase 2 - Inbound Sync

- Board/Card Polling auf Basis der Deck-API
- Erkennung relevanter Karten fuer Hermes
- Diff-/Aenderungserkennung
- Loop-Schutz fuer Hermes-eigene Writes

### Phase 3 - Pairing und Mapping

- Board-Metadaten speichern
- Status-Mapping-Modell implementieren
- Onboarding-/Pairing-Flow fuer ungepairte Boards

### Phase 4 - Writeback

- Kommentare schreiben
- Stack-Wechsel
- Checkboxen in Beschreibungen robust aktualisieren

### Phase 5 - Optionale Talk-Spiegelung

- Ausgabe ueber bestehenden Talk-Kanal
- pro Board aktivierbar

### Phase 6 - Tests und Doku

- Mapping-Tests
- Change-detection-Tests
- Loop-Schutz
- Checkbox-Mutation mit Konfliktfaellen

## Bewusste Nicht-Ziele in dieser Phase

- direkte Nextcloud-Tasks-CalDAV-Integration
- universelle Unterstuetzung beliebiger Task- oder Kanban-Systeme
- tiefe Kopplung an das interne Hermes-Desktop-Kanban

## Empfehlung fuer die naechste Freigabe

Wenn umgesetzt werden soll, dann in dieser Reihenfolge:

1. eigene Plattform `nextcloud-deck`
2. Board-Pairing mit persistiertem Stack-Mapping
3. Kommentar- und Stack-Writeback
4. Checkbox-Updates in Beschreibungen
5. optionale Talk-Spiegelung
