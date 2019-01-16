{ mkDerivation, aeson-pretty, base, bytestring, containers, dhall
, discrimination, foldl, Frames, Frames-dsv, groups, hvega, lens
, mtl, pipes, prettyprinter, profunctors, random-fu, random-source
, stdenv, text, type-of-html, vector, vinyl
}:
mkDerivation {
  pname = "incarceration";
  version = "0.1.0.0";
  src = /Users/adam/DataScience/incarceration;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    base containers dhall discrimination foldl Frames groups hvega lens
    mtl pipes profunctors random-fu random-source text vector vinyl
  ];
  executableHaskellDepends = [
    aeson-pretty base bytestring containers dhall foldl Frames
    Frames-dsv hvega lens mtl pipes prettyprinter random-fu
    random-source text type-of-html vector vinyl
  ];
  description = "Explore incarceration stats from Vera Incarceration Trends dataset";
  license = stdenv.lib.licenses.bsd3;
}
