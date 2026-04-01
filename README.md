# katana_sync
KATANA KRONOS – Enterprise Backup Solution with GUI, rsync integration, scheduling, versioning and restore system.

<<<<<<< HEAD
=======
---

python3 -m venv myenv
source myenv/bin/activate
python3 katana_sync.py

---

>>>>>>> 498e466 (Update)
# ⚔️ KATANA KRONOS – Enterprise Backup Solution

**Katana Kronos** (Version 7.1) ist eine leistungsstarke und einfach zu bedienende Backup-Software für Linux-Systeme. Sie kombiniert die Zuverlässigkeit des Profi-Werkzeugs `rsync` mit einer modernen, dunklen Benutzeroberfläche.

## ✨ Hauptfunktionen

* **⚡ Cockpit**: Ein übersichtliches Dashboard zur Steuerung deiner Datensicherungen.
* **🕒 Autopilot**: Erstelle automatische Zeitpläne (täglich oder wöchentlich), damit du dich um nichts mehr kümmern musst.
* **🔄 Restore-System**: Stelle gelöschte oder alte Dateien ganz einfach aus automatischen Snapshots wieder her.
* **📂 Storage Commander**: Ein intelligenter Assistent, der dir hilft, die richtigen Festplatten und Ordner zu finden.
* **📜 Logbuch**: Jede Aktion wird genau mitgeschrieben, damit du immer die volle Kontrolle hast.

## 🛠️ Backup-Strategien

Das Programm bietet verschiedene Profi-Einstellungen:
* **Mirroring**: Erstellt eine exakte Kopie deines Ordners.
* **Versioning**: Speichert alte Dateiversionen sicher in einem Archiv (`_Archiv`), statt sie zu löschen.
* **Smart Clean**: Ignoriert automatisch unnötigen Müll wie Papierkörbe oder temporäre Dateien.
* **Integrity Check**: Prüft den Inhalt deiner Dateien auf Fehler.
* **Manifest**: Erstellt eine Liste mit digitalen Fingerabdrücken (SHA256) für maximale Datensicherheit.

## 🚀 Installation & Start

### Voraussetzungen
Da Katana Kronos auf bewährte Linux-Technik setzt, müssen folgende Pakete installiert sein:
* **Python 3** (inklusive `tkinter` für das Fenster)
* **rsync** (für die eigentliche Kopierarbeit)

### Starten
Du kannst das Programm direkt über das Terminal starten:
```bash
python3 katana_sync.py
```
*Tipp: Du findest im Programm einen Knopf, um Katana Kronos automatisch mit deinem Computer mitstarten zu lassen.*

---

## 📝 Technische Details
* **Konfiguration**: Wird in `~/.katana_sync_config.json` gespeichert.
* **Log-Datei**: Zu finden unter `~/katana_backup.log`.

---
**Entwickelt von ghostfanman**

---

### So erstellst du die Datei auf deinem Computer:

1.  Erstelle im Ordner `katana_sync` eine neue Datei mit dem Namen `README.md`.
2.  Kopiere den Text von oben hinein und speichere sie.
3.  Lade sie mit diesen Befehlen hoch:
    * `git add README.md`
    * `git commit -m "README hinzugefügt"`
    * `git push`

Sieht das für dich so stimmig aus oder soll ich noch einen speziellen Hinweis einbauen?
