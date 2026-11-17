const target = new EventTarget();

export function on(evt, fn) {
  const handler = (e) => fn(e.detail, e);
  target.addEventListener(evt, handler);
  return () => target.removeEventListener(evt, handler);
}

export function emit(evt, detail) {
  target.dispatchEvent(new CustomEvent(evt, { detail }));
}
