{
  inputs = {
    utils.url = "github:numtide/flake-utils";
  };
  outputs =
    {
      self,
      nixpkgs,
      utils,
    }:
    utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShell = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            rustfmt
            bear
            rustc
            cargo
            rust-analyzer
            pkg-config
            gcc
            mbuffer
            # deploy: ansible-core only (playbook uses builtin modules, no collections)
            ansible
            ansible-lint
          ];

          buildInputs = with pkgs; [
            pv
            zeromq
            gst_all_1.gstreamer
            gst_all_1.gst-plugins-base
            gst_all_1.gst-plugins-good
            gst_all_1.gst-plugins-bad
            gst_all_1.gst-libav
            (python3.withPackages (ps: [
              ps.numpy
              ps.pyzmq
            ]))

            mosquitto
            socat
          ];

          shellHook = ''
            export PYTHONPATH="${pkgs.gnuradio}/${pkgs.python3.sitePackages}''${PYTHONPATH:+:$PYTHONPATH}"
          '';
        };
      }
    );
}
