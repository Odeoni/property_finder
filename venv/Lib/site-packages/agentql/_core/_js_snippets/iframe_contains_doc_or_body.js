() => {
  const doc = document;
  if (!doc || !doc.body) {
    return false;
  }
  return doc.body.hasChildNodes();
};
