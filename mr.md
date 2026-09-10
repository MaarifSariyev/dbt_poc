changed_files/
├── .gitignore                              (yeni)
├── backend/config/settings.py              (dəyişib)
├── backend/config/urls.py                  (dəyişib)
└── backend/core/
    ├── services.py                         (dəyişib)
    ├── views.py                            (dəyişib)
    ├── ip_intelligence.py                  (yeni)
    └── data/
        ├── vpn_ipv4.txt                    (yeni)
        └── datacenter_ipv4.txt             (yeni)
Necə istifadə edəsən
Zip-i aç
team-capx reponu klonlamısansa, içindəki faylları eyni yollara köçür (üzərinə yaz) — məsələn changed_files/backend/core/services.py → team-capx/backend/core/services.py
Sonra:
bash
cd team-capx
git checkout -b feature/real-ip-fraud-detection
git add .gitignore backend/config/settings.py backend/config/urls.py backend/core/services.py backend/core/views.py backend/core/ip_intelligence.py backend/core/data/
git commit -m "Add real IP-based VPN/datacenter detection"
git push -u origin feature/real-ip-fraud-detection

Push zamanı username soruşsa, password yerinə token-i yapışdır.
