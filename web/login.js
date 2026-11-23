(() => {
  const $ = s => document.querySelector(s);
  const $$ = s => document.querySelectorAll(s);

  // ─── Redirect if already logged in ───
  if (localStorage.getItem('token')) {
    window.location.href = '/chat.html';
  }

  // ─── Falling Z animation ───
  const canvas = $('#z-canvas');

  function createFallingZ() {
    const z = document.createElement('span');
    z.className = 'falling-z';
    z.textContent = 'Z';

    const size = Math.random() * 70 + 30;
    const left = Math.random() * 100;
    const duration = Math.random() * 8 + 10;
    const delay = Math.random() * -16;
    const opacity = Math.random() * 0.12 + 0.06;
    const rotation = (Math.random() - 0.5) * 50;

    z.style.cssText = `
      font-size: ${size}px;
      left: ${left}%;
      animation-duration: ${duration}s;
      animation-delay: ${delay}s;
      --z-opacity: ${opacity};
      --z-rot: ${rotation}deg;
    `;

    canvas.appendChild(z);
  }

  for (let i = 0; i < 25; i++) {
    createFallingZ();
  }

  // ─── View switching ───
  const viewReg = $('#view-register');
  const viewLogin = $('#view-login');
  const success = $('#success');

  function switchView(from, to) {
    from.classList.add('hidden');
    to.classList.remove('hidden');
    to.classList.remove('fade-in');
    void to.offsetWidth;
    to.classList.add('fade-in');
    $$('.form-group.error').forEach(g => g.classList.remove('error'));
    $$('.server-error').forEach(el => el.textContent = '');
  }

  $('#go-login').addEventListener('click', () => switchView(viewReg, viewLogin));
  $('#go-register').addEventListener('click', () => switchView(viewLogin, viewReg));

  // ─── Toggle password ───
  $$('.toggle-pass').forEach(btn => {
    btn.addEventListener('click', () => {
      const input = $('#' + btn.dataset.target);
      const isPass = input.type === 'password';
      input.type = isPass ? 'text' : 'password';
      btn.innerHTML = isPass
        ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19M1 1l22 22"/></svg>`
        : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`;
    });
  });

  // ─── Password strength ───
  const regPass = $('#reg-pass');
  const strBar = $('#str-bar');
  const strLabel = $('#str-label');
  const strengthTexts = ['', 'Слабый', 'Средний', 'Хороший', 'Отличный'];

  function getStrength(p) {
    let s = 0;
    if (p.length >= 6) s++;
    if (p.length >= 10) s++;
    if (/[A-Z]/.test(p) && /[a-z]/.test(p)) s++;
    if (/[0-9]/.test(p) && /[^A-Za-z0-9]/.test(p)) s++;
    return s;
  }

  regPass.addEventListener('input', () => {
    $('#fg-reg-pass').classList.remove('error');
    const s = regPass.value.length === 0 ? 0 : getStrength(regPass.value);
    strBar.className = 'strength-bar' + (s ? ' s' + s : '');
    strLabel.textContent = strengthTexts[s];
  });

  $$('.form-group input').forEach(inp => {
    inp.addEventListener('input', () => inp.closest('.form-group').classList.remove('error'));
  });

  // ─── Register ───
  function authSnapshot() {
    return { token: localStorage.getItem('token'), revision: localStorage.getItem('zchat_auth_revision') };
  }

  async function acceptLogin(data, snapshot) {
    const commit = () => {
      if (localStorage.getItem('token') !== snapshot.token || localStorage.getItem('zchat_auth_revision') !== snapshot.revision) return false;
      localStorage.setItem('zchat_auth_revision', Date.now() + ':' + Math.random());
      localStorage.setItem('username', data.username);
      localStorage.setItem('avatar_url', data.avatar_url || '');
      localStorage.setItem('token', data.token);
      return true;
    };
    const accepted = navigator.locks ? await navigator.locks.request('zchat-auth-state', commit) : commit();
    if (!accepted) {
      // A different tab already changed the login. Discard this newly issued session.
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 10000);
      try { await fetch('/api/profile/logout', {method: 'POST', headers: {Authorization: 'Bearer ' + data.token}, signal: controller.signal}); } catch {}
      finally { clearTimeout(timer); }
      window.location.href = localStorage.getItem('token') ? '/chat.html' : '/login.html';
      return false;
    }
    return true;
  }

  $('#btn-register').addEventListener('click', async () => {
    if ($('#btn-register').classList.contains('loading')) return;
    const snapshot = authSnapshot();
    if (snapshot.token) { window.location.href = '/chat.html'; return; }
    const email = $('#reg-email');
    const user = $('#reg-user');
    const pass = $('#reg-pass');
    const pass2 = $('#reg-pass2');
    const serverError = $('#reg-server-error');
    serverError.textContent = '';
    let valid = true;

    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.value.trim())) {
      $('#fg-reg-email').classList.add('error'); valid = false;
    }
    if (user.value.trim().length < 3) {
      $('#fg-reg-user').classList.add('error'); valid = false;
    }
    if (pass.value.length < 6) {
      $('#fg-reg-pass').classList.add('error'); valid = false;
    }
    if (pass2.value !== pass.value || pass2.value.length === 0) {
      $('#fg-reg-pass2').classList.add('error'); valid = false;
    }
    if (!valid) return;

    const btn = $('#btn-register');
    btn.classList.add('loading');

    try {
      const res = await fetch('/api/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: user.value.trim(),
          email: email.value.trim(),
          password: pass.value,
        }),
      });

      btn.classList.remove('loading');

      if (res.ok) {
        const data = await res.json();
        if (!await acceptLogin(data, snapshot)) return;

        viewReg.classList.add('hidden');
        $('#success-title').textContent = 'Готово!';
        $('#success-sub').textContent = 'Аккаунт успешно создан';
        success.classList.add('show');

        setTimeout(() => {
          window.location.href = '/chat.html';
        }, 1200);
      } else {
        serverError.textContent = res.status === 409 ? 'Имя пользователя или email уже заняты' : res.status === 400 ? 'Проверьте имя, email и пароль (от 6 символов до 72 байт)' : res.status === 429 ? 'Слишком много попыток. Подождите немного.' : 'Регистрация временно недоступна. Попробуйте позже.';
      }
    } catch (err) {
      btn.classList.remove('loading');
      serverError.textContent = 'Ошибка соединения с сервером';
    }
  });

  // ─── Login ───
  $('#btn-login').addEventListener('click', async () => {
    if ($('#btn-login').classList.contains('loading')) return;
    const snapshot = authSnapshot();
    if (snapshot.token) { window.location.href = '/chat.html'; return; }
    const emailInput = $('#login-email');
    const pass = $('#login-pass');
    const serverError = $('#login-server-error');
    serverError.textContent = '';
    let valid = true;

    if (emailInput.value.trim().length < 1) {
      $('#fg-login-email').classList.add('error'); valid = false;
    }
    if (pass.value.length < 1) {
      $('#fg-login-pass').classList.add('error'); valid = false;
    }
    if (!valid) return;

    const btn = $('#btn-login');
    btn.classList.add('loading');

    try {
      const res = await fetch('/api/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: emailInput.value.trim(),
          password: pass.value,
        }),
      });

      btn.classList.remove('loading');

      if (res.ok) {
        const data = await res.json();
        if (!await acceptLogin(data, snapshot)) return;

        viewLogin.classList.add('hidden');
        $('#success-title').textContent = 'Добро пожаловать!';
        $('#success-sub').textContent = 'Вы успешно вошли в аккаунт';
        success.classList.add('show');

        setTimeout(() => {
          window.location.href = '/chat.html';
        }, 1000);
      } else {
        serverError.textContent = res.status === 401 ? 'Неверный email или пароль' : res.status === 429 ? 'Слишком много попыток. Подождите немного.' : 'Вход временно недоступен. Попробуйте позже.';
      }
    } catch (err) {
      btn.classList.remove('loading');
      serverError.textContent = 'Ошибка соединения с сервером';
    }
  });

  // ─── Google (unavailable) ───
  function showToast(msg) {
    const toast = $('#toast');
    toast.textContent = msg;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 2500);
  }

  const loginNotice = sessionStorage.getItem('zchat_login_notice');
  if (loginNotice) {
    sessionStorage.removeItem('zchat_login_notice');
    showToast(loginNotice);
  }

  $$('.btn-google').forEach(btn => {
    btn.addEventListener('click', () => {
      showToast('Google OAuth пока недоступен');
    });
  });

  // ─── Enter key ───
  document.addEventListener('keydown', e => {
    if (e.key !== 'Enter') return;
    if (!viewReg.classList.contains('hidden')) $('#btn-register').click();
    else if (!viewLogin.classList.contains('hidden')) $('#btn-login').click();
  });
})();
