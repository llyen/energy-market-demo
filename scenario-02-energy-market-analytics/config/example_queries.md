# Przykładowe zapytania do Data Agenta: Analityk Rynku Energii

Poniższe zapytania można skopiować do konfiguracji Fabric Data Agent jako przykłady
(sekcja *Example queries*), a także wykorzystać do testowania agenta.

---

## 🏷️ Ceny rynkowe

1. Jaka była średnia cena energii na RDN w styczniu tego roku?
2. Pokaż 10 najdroższych godzin w ostatnim kwartale.
3. Jaka jest różnica cenowa między szczytem (7–21) a doliną (22–6) w dni robocze?
4. Jak zmieniała się średnia cena spot miesiąc do miesiąca w ostatnim roku?
5. Ile wyniosła maksymalna cena energii w ostatnim roku i kiedy wystąpiła?
6. Jaki jest średni wolumen obrotu na RDN w godzinach szczytu?
7. Pokaż rozkład cen na RDN w przedziale 0–200, 200–400, 400–600, 600–800, powyżej 800 PLN/MWh.

## ⚡ Miks energetyczny

8. Jaki jest średni udział OZE w generacji w bieżącym roku?
9. Porównaj generację z wiatru latem (czerwiec–sierpień) i zimą (grudzień–luty).
10. Kiedy fotowoltaika generowała najwięcej energii? Podaj datę i wartość w MW.
11. Ile wynosi średnia generacja z węgla w godzinach nocnych (0:00–5:00)?
12. Jak zmienia się miks energetyczny w weekendy w porównaniu do dni roboczych?
13. Jaki jest trend udziału OZE w generacji — pokaż wartość kwartalną.
14. Od kiedy i na jakim poziomie pojawia się generacja jądrowa w danych?
15. Ile wynosi średnia łączna generacja (total_mw) w szczycie vs poza szczytem?

## 📑 Kontrakty bilateralne

16. Ile aktywnych kontraktów typu Baseload jest w portfelu?
17. Który uczestnik rynku (buyer) kupił najwięcej energii w MWh?
18. Jaka jest średnia cena kontraktów kwartalnych (Quarter) w porównaniu do rocznych (Year)?
19. Pokaż rozkład kontraktów według typu produktu (Month/Quarter/Year) i statusu.
20. Ile wynosi łączna wartość rozliczeń (settlement_pln) aktywnych kontraktów?
21. Który sprzedawca (seller) ma największy wolumen kontraktów?
22. Jaki procent kontraktów został anulowany (Cancelled)?

## 🌍 Emisje CO₂

23. Jaka jest średnia intensywność emisji CO₂ (tCO₂/MWh) w ostatnim kwartale?
24. Jak zmieniał się koszt emisji CO₂ (carbon_cost_pln) miesiąc do miesiąca?
25. Jaki jest trend ceny uprawnień EU ETS w analizowanym okresie?
26. Porównaj emisje w miesiącach letnich (VI–VIII) i zimowych (XII–II).
27. Jaki jest udział emisji z węgla vs gazu w łącznych emisjach?

## 📊 Analizy porównawcze i przekrojowe

28. Porównaj udział OZE w miksie energetycznym w Q1 vs Q2 bieżącego roku.
29. Jak zmienił się miks energetyczny rok do roku (pierwszy rok vs drugi rok danych)?
30. Czy istnieje korelacja między ceną spot a udziałem OZE w generacji?
31. Porównaj wolumeny kontraktów kupna PGE Obrót i Tauron Sprzedaż.
32. Która pora roku (Q1/Q2/Q3/Q4) ma najniższe średnie ceny energii?
33. Jak cena EU ETS koreluje z kosztami emisji w Polsce?
34. Pokaż zależność między generacją wiatrową a ceną spot — czy więcej wiatru oznacza niższą cenę?
35. Zestawienie: średnia cena spot, udział OZE, emisje CO₂ — kwartał po kwartale.
