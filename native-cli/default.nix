{pkgs, ...}:
pkgs.buildGoModule {
  pname = "native-cli";
  version = "0.3.0";
  src = ./.;
  vendorHash = "sha256-PseP9FP0PhHVGr86QYJwGrbbC172SgBTPcfy/1y392o=";
  buildInputs = [pkgs.makeWrapper];
  installPhase = ''
    runHook preInstall
    install -D $GOPATH/bin/native-cli $out/bin/native
    runHook postInstall
  '';
  postInstall = let
    pulumiPath = pkgs.lib.makeBinPath [
      (pkgs.pulumi.withPackages (ps: [ps.pulumi-language-go]))
    ];
  in ''
    wrapProgram $out/bin/native --prefix PATH : ${pulumiPath}
  '';
  meta = with pkgs.lib; {
    description = "NativeLink development cluster.";
    homepage = "https://github.com/TraceMachina/nativelink";
    license = licenses.asl20;
    maintainers = [maintainers.aaronmondal];
  };
}
