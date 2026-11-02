/* Mesure d'audience (02/11/2026) : Google Analytics 4 ne se charge qu'apres « Accepter ».
   Le choix est garde six mois (cookie `noyaru_consentement`) ; « Gerer les cookies » le rouvre.
   Refuser apres avoir accepte efface les cookies _ga et recharge la page, pour que plus rien
   de Google ne tourne. */
(function () {
  var boite = document.getElementById('consentement-mesure');
  if (!boite) return;
  var id = boite.getAttribute('data-ga4');
  var NOM = 'noyaru_consentement';
  var SIX_MOIS = 180 * 24 * 3600;
  // 13 mois au plus pour un cookie de mesure (recommandation CNIL) ; Google en met 24 par defaut.
  var TREIZE_MOIS = 395 * 24 * 3600;

  function choix() {
    var m = document.cookie.match(/(?:^|;\s*)noyaru_consentement=([^;]+)/);
    return m ? m[1] : '';
  }
  function retenir(valeur) {
    document.cookie = NOM + '=' + valeur + '; Max-Age=' + SIX_MOIS + '; Path=/; SameSite=Lax' +
      (location.protocol === 'https:' ? '; Secure' : '');
  }
  function charger() {
    if (window.__noyaruMesure) return;
    window.__noyaruMesure = true;
    window.dataLayer = window.dataLayer || [];
    window.gtag = function () { window.dataLayer.push(arguments); };
    window.gtag('js', new Date());
    window.gtag('config', id, { cookie_expires: TREIZE_MOIS });
    var s = document.createElement('script');
    s.async = true;
    s.src = 'https://www.googletagmanager.com/gtag/js?id=' + encodeURIComponent(id);
    document.head.appendChild(s);
  }
  function effacerLesCookiesDeMesure() {
    var hote = location.hostname;
    document.cookie.split(';').forEach(function (c) {
      var nom = c.split('=')[0].trim();
      if (!/^_ga(_|$)/.test(nom)) return;
      ['', '; Domain=' + hote, '; Domain=.' + hote.replace(/^www\./, '')].forEach(function (d) {
        document.cookie = nom + '=; Max-Age=0; Path=/' + d;
      });
    });
  }

  // Le serveur cache deja le bandeau quand il voit le choix ; une page servie depuis un cache ne
  // le voit pas : le script le cache aussi (sonde Chromium du 02/11/2026).
  if (choix() === 'accepte' || choix() === 'refuse') boite.hidden = true;
  if (choix() === 'accepte') charger();

  boite.addEventListener('click', function (e) {
    var bouton = e.target.closest('[data-consentement]');
    if (!bouton) return;
    var valeur = bouton.getAttribute('data-consentement');
    var avaitCharge = Boolean(window.__noyaruMesure);
    retenir(valeur);
    boite.hidden = true;
    if (valeur === 'accepte') { charger(); return; }
    effacerLesCookiesDeMesure();
    if (avaitCharge) location.reload();
  });

  document.addEventListener('click', function (e) {
    var lien = e.target.closest('[data-gerer-cookies]');
    if (!lien) return;
    e.preventDefault();
    boite.hidden = false;
    var premier = boite.querySelector('[data-consentement]');
    if (premier) premier.focus();
  });
})();
